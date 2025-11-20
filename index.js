// Load environment variables from server/.env regardless of process CWD
try {
  const path = require('path');
  require('dotenv').config({ path: path.join(__dirname, '.env') });
} catch {}
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const compression = require('compression');
const rateLimit = require('express-rate-limit');
const morgan = require('morgan');
const hpp = require('hpp');
const app = express();
const { randomUUID } = require('crypto');
app.set('trust proxy', 1);
// Security and performance middlewares
app.use(helmet({
  crossOriginResourcePolicy: { policy: 'cross-origin' },
}));
app.use(hpp());
app.use(compression());
app.use(cors());
app.use(morgan('combined'));
// Basic rate limiting for all routes; sensitive routes can override if needed
const limiter = rateLimit({ windowMs: 60 * 1000, max: 600, standardHeaders: true, legacyHeaders: false });
app.use(limiter);
// Request timeout safeguard (per request)
app.use((req, res, next) => {
  // 25s soft timeout
  req.setTimeout(25_000, () => {});
  res.setTimeout(25_000, () => {});
  next();
});
// Increase JSON body limit to allow base64 images up to 5MB (base64 ~33% overhead)
app.use(express.json({ limit: '7mb' }));
app.use(express.urlencoded({ extended: true, limit: '7mb' }));
// Utilities for mail and calendar
const { sendEmail, verifySmtp } = require('./utils/mailer');
const { generateICS, generateGoogleCalendarLink, generateICSMultiForReminders, generateGoogleImportByIcsUrl } = require('./utils/calendar');
const { meetingEmailHtml } = require('./utils/templates/meetingEmail');
const { remindersEmailHtml } = require('./utils/templates/remindersEmail');
const { hashPassword, verifyPassword, signToken, requireAuth, requireRole, ownerExists } = require('./auth');
// Feature flags for optional schema parts; refreshed on startup
const featureFlags = {
  hasSector: false,
  hasLocationUrl: false,
  hasImages: false,
};
async function refreshFeatureFlags(db) {
  try {
    const cols = await db.query(`
      SELECT column_name FROM information_schema.columns
       WHERE table_schema='public' AND table_name='opportunities'
    `);
    const names = new Set(cols.rows.map(r => String(r.column_name).toLowerCase()));
    featureFlags.hasSector = names.has('sector');
    featureFlags.hasLocationUrl = names.has('location_url');
  } catch (e) {
    // ignore
  }
  try {
    const r = await db.query(`SELECT to_regclass('public.opportunity_images') AS reg`);
    featureFlags.hasImages = !!(r.rows && r.rows[0] && r.rows[0].reg);
  } catch (e) {
    featureFlags.hasImages = false;
  }
  // Detect reminders email audit tables to enable aggregates safely without env flags
  try {
    const resa = await db.query(`SELECT to_regclass('public.reminder_email_selected_audit') AS reg`);
    featureFlags.hasRemindersEmailAudit = !!(resa.rows && resa.rows[0] && resa.rows[0].reg);
  } catch (e) {
    featureFlags.hasRemindersEmailAudit = false;
  }
  // Detect reminder call attempt audit table
  try {
    const rca = await db.query(`SELECT to_regclass('public.reminder_call_attempt_audit') AS reg`);
    featureFlags.hasRemindersCallAudit = !!(rca.rows && rca.rows[0] && rca.rows[0].reg);
  } catch (e) {
    featureFlags.hasRemindersCallAudit = false;
  }
}

// Ensure combined view for admin/owner employee profiles exists (idempotent)
async function ensureUserFullProfilesView(db) {
  try {
    await db.query(`
      CREATE OR REPLACE VIEW public.user_full_profiles AS
      SELECT 
        u.id AS user_id,
        u.full_name,
        u.username,
        u.email,
        u.phone,
        u.role,
        u.joining_date,
        u.status,
        p.date_of_birth,
        p.gender,
        p.emergency_contact_name,
        p.emergency_contact_phone,
        p.address,
        p.pan,
        p.pan_normalized,
        p.aadhaar,
        p.aadhaar_last4,
        p.updated_at
      FROM public.users u
      LEFT JOIN public.user_profiles p ON p.user_id = u.id;
    `);
  } catch (e) {
    // Do not crash server; endpoint callers can retry
    if (!process.env.SUPPRESS_DB_LOG) console.warn('[ensureUserFullProfilesView] warning:', e.message);
  }
}

// Self-healing minimal schema to guarantee new columns/tables exist in current DB
async function ensureMinimalSchema(db) {
  try {
    // Attempt pgcrypto enable; ignore if provider blocks extensions
    try { await db.query("CREATE EXTENSION IF NOT EXISTS pgcrypto"); }
    catch (e) { if (!process.env.SUPPRESS_DB_LOG) console.warn('[ensureMinimalSchema] pgcrypto warn:', e.message); }

    // opportunities.sector and opportunities.location_url
    await db.query("ALTER TABLE public.opportunities ADD COLUMN IF NOT EXISTS sector TEXT NULL");
    await db.query("ALTER TABLE public.opportunities ADD COLUMN IF NOT EXISTS location_url TEXT NULL");
    // Replace sector CHECK with expanded allowlist
    await db.query("ALTER TABLE public.opportunities DROP CONSTRAINT IF EXISTS opportunities_sector_check");
    await db.query("ALTER TABLE public.opportunities ADD CONSTRAINT opportunities_sector_check CHECK (sector IS NULL OR sector IN ('CONSTRUCTION','MINING','HOSPITAL & HEALTHCARE','COMMERCIAL','INSTITUTIONAL','LOGISTICS','INDUSTRIAL','RESIDENTIAL','AGRICULTURE','OTHER'))");

    // opportunity_images table + indexes
    await db.query("CREATE TABLE IF NOT EXISTS public.opportunity_images (id BIGSERIAL PRIMARY KEY, opportunity_id VARCHAR(20) NOT NULL, mime_type TEXT NOT NULL, file_name TEXT, file_size_bytes INTEGER NOT NULL, data BYTEA NOT NULL, created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(), created_by TEXT NULL, created_by_user_id UUID NULL, CONSTRAINT fk_opp_images_opp FOREIGN KEY (opportunity_id) REFERENCES public.opportunities(opportunity_id) ON DELETE CASCADE, CONSTRAINT chk_opp_images_size CHECK (file_size_bytes >= 0 AND file_size_bytes <= 5*1024*1024))");
    await db.query("CREATE INDEX IF NOT EXISTS idx_opp_images_opp ON public.opportunity_images(opportunity_id)");
    await db.query("CREATE INDEX IF NOT EXISTS idx_opp_images_created_at ON public.opportunity_images(created_at DESC)");
    // Helpful indexes for list endpoints
    await db.query("CREATE INDEX IF NOT EXISTS idx_opportunities_stage ON public.opportunities(stage)");
    await db.query("CREATE INDEX IF NOT EXISTS idx_opportunities_salesperson ON public.opportunities(salesperson)");
    await db.query("CREATE INDEX IF NOT EXISTS idx_targets_status ON public.targets(status)");
    await db.query("CREATE INDEX IF NOT EXISTS idx_meetings_starts_at ON public.meetings(starts_at DESC)");
    await db.query("CREATE INDEX IF NOT EXISTS idx_reminders_due_ts ON public.reminders(due_ts DESC)");
    await db.query("CREATE INDEX IF NOT EXISTS idx_customers_created_at ON public.customers(created_at DESC)");
    await db.query("CREATE INDEX IF NOT EXISTS idx_customers_client_name ON public.customers(client_name)");
    await db.query("CREATE INDEX IF NOT EXISTS idx_contracts_created_at ON public.contracts(created_at DESC)");
    await db.query("CREATE INDEX IF NOT EXISTS idx_contracts_client_name ON public.contracts(client_name)");

    // meetings.meeting_link (optional URL for online meeting)
    try {
      await db.query("ALTER TABLE public.meetings ADD COLUMN IF NOT EXISTS meeting_link TEXT NULL");
    } catch (e) {
      if (!process.env.SUPPRESS_DB_LOG) console.warn('[ensureMinimalSchema] meeting_link warn:', e.message);
    }

    // Minimal users table to unblock auth if migrations haven't run yet
    await db.query(`
      CREATE TABLE IF NOT EXISTS public.users (
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
      )
    `);
    await db.query("CREATE INDEX IF NOT EXISTS idx_users_role ON public.users(role)");
    await db.query("CREATE INDEX IF NOT EXISTS idx_users_active ON public.users(active)");

    // Trips per truck per day (to support multiple depot trips in a day)
    await db.query(`
      CREATE TABLE IF NOT EXISTS public.truck_dispenser_trips (
        id BIGSERIAL PRIMARY KEY,
        truck_id INTEGER NOT NULL REFERENCES public.storage_units(id) ON DELETE CASCADE,
        reading_date DATE NOT NULL,
        trip_no INTEGER NOT NULL CHECK (trip_no > 0),
        opening_liters INTEGER NOT NULL DEFAULT 0 CHECK (opening_liters >= 0),
        closing_liters INTEGER NOT NULL DEFAULT 0 CHECK (closing_liters >= 0),
        opening_at TIMESTAMP WITHOUT TIME ZONE NULL,
        closing_at TIMESTAMP WITHOUT TIME ZONE NULL,
        note TEXT,
        driver_name TEXT,
        driver_code TEXT,
        created_by TEXT,
        created_by_user_id UUID,
        created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_trips_per_day UNIQUE (truck_id, reading_date, trip_no)
      )`);
    await db.query("CREATE INDEX IF NOT EXISTS idx_trips_truck_date ON public.truck_dispenser_trips(truck_id, reading_date, trip_no)");
    // Create new dispenser day reading logs table (lightweight audit of opening/closing readings)
    await db.query(`
      CREATE TABLE IF NOT EXISTS public.dispenser_day_reading_logs (
        id BIGSERIAL PRIMARY KEY,
        truck_id INTEGER NOT NULL REFERENCES public.storage_units(id) ON DELETE CASCADE,
        truck_code TEXT NULL,
        reading_date DATE NOT NULL,
        opening_liters INTEGER NOT NULL DEFAULT 0 CHECK (opening_liters >= 0),
        opening_at TIMESTAMP WITHOUT TIME ZONE NULL,
        closing_liters INTEGER NULL,
        closing_at TIMESTAMP WITHOUT TIME ZONE NULL,
        note TEXT,
        driver_name TEXT,
        driver_code TEXT,
        created_by TEXT,
        created_by_user_id UUID,
        created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_dispenser_day_unique UNIQUE (truck_id, reading_date)
      )
    `);
    await db.query("CREATE INDEX IF NOT EXISTS idx_dispenser_day_truck_date ON public.dispenser_day_reading_logs(truck_id, reading_date)");
    // Ensure truck_code column exists for older DBs and populate from storage_units when possible
    try {
      await db.query("ALTER TABLE public.dispenser_day_reading_logs ADD COLUMN IF NOT EXISTS truck_code TEXT NULL");
      await db.query("CREATE INDEX IF NOT EXISTS idx_dispenser_day_truck_code ON public.dispenser_day_reading_logs(truck_code)");
      // Backfill existing rows where truck_code is NULL
      await db.query("UPDATE public.dispenser_day_reading_logs SET truck_code = (SELECT unit_code FROM public.storage_units su WHERE su.id = public.dispenser_day_reading_logs.truck_id) WHERE truck_code IS NULL");
    } catch (e) { if (!process.env.SUPPRESS_DB_LOG) console.warn('[ensureMinimalSchema dispenser_day truck_code warn]', e.message); }
    // Create testing_self_transfers table to record same-tanker testing events separately
    await db.query(`
      CREATE TABLE IF NOT EXISTS public.testing_self_transfers (
        id BIGSERIAL PRIMARY KEY,
        lot_id BIGINT NULL REFERENCES public.fuel_lots(id) ON DELETE CASCADE,
        activity TEXT NOT NULL,
        from_unit_id INTEGER NULL REFERENCES public.storage_units(id) ON DELETE RESTRICT,
        from_unit_code TEXT NULL,
        to_vehicle TEXT NULL,
        transfer_volume_liters INTEGER NOT NULL,
        lot_code TEXT NULL,
        driver_id INTEGER NULL REFERENCES public.drivers(id) ON DELETE SET NULL,
        driver_name TEXT NULL,
        performed_by TEXT NULL,
        performed_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
        updated_by TEXT NULL,
        sale_date DATE NULL,
        trip INTEGER NULL,
        created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
      )
    `);
    await db.query("CREATE INDEX IF NOT EXISTS idx_testing_self_from_unit ON public.testing_self_transfers(from_unit_id)");
  } catch (e) {
    console.warn('[ensureMinimalSchema] warning:', e.message);
  }
}
function buildOpportunitySelectFields(includeSpend = true) {
  const fields = [
    'o.opportunity_id',
    'o.client_name',
    'o.purpose',
    'o.expected_monthly_volume_l',
    'o.proposed_price_per_litre',
  ];
  if (featureFlags.hasSector) fields.push('o.sector');
  if (featureFlags.hasLocationUrl) fields.push('o.location_url');
  fields.push('o.stage','o.probability','o.notes','o.salesperson','o.assignment');
  if (includeSpend) fields.push('COALESCE(x.total, 0) AS spend');
  fields.push('o.loss_reason');
  if (featureFlags.hasImages) {
    fields.push(`EXISTS (SELECT 1 FROM public.opportunity_images i WHERE i.opportunity_id = o.opportunity_id) AS has_image`);
    fields.push(`(SELECT id FROM public.opportunity_images i WHERE i.opportunity_id = o.opportunity_id ORDER BY i.created_at DESC LIMIT 1) AS first_image_id`);
  }
  return fields;
}

// Helper to derive a human-readable actor label for audit logs
function getActor(req) {
  if (req && req.user) {
    // Prefer username for display/canonical text, fall back to email
    return req.user.username || req.user.email || req.user.sub || 'user';
  }
  return 'user';
}
// Normalize an assignee label to a canonical identifier (prefer email, else username, else full_name as last resort)
async function normalizeAssigneeValue(value) {
  if (!value) return null;
  const v = String(value).trim();
  try {
    const r = await pool.query(
      `SELECT email, username, full_name FROM public.users
       WHERE active=TRUE AND (
         LOWER(COALESCE(email,'')) = LOWER($1)
         OR LOWER(COALESCE(username,'')) = LOWER($1)
         OR LOWER(COALESCE(full_name,'')) = LOWER($1)
       )
       LIMIT 1`,
      [v]
    );
    if (r.rows.length) {
      const u = r.rows[0];
      return u.email || u.username || u.full_name || v;
    }
  } catch (e) {
    // fall through
  }
  return v;
}

// Resolve a user by email/username/full_name (case-insensitive). Returns full user row or null.
async function resolveUserByIdentifier(val) {
  if (!val) return null;
  const v = String(val).trim();
  if (!v) return null;
  const r = await pool.query(
    `SELECT id, email, username, full_name, role, active
       FROM public.users
      WHERE active=TRUE AND (
        LOWER(COALESCE(email,'')) = LOWER($1)
        OR LOWER(COALESCE(username,'')) = LOWER($1)
        OR LOWER(COALESCE(full_name,'')) = LOWER($1)
      )
      LIMIT 1`,
    [v]
  );
  return r.rows[0] || null;
}

function pickDisplay(u) {
  if (!u) return null;
  // Always prefer username for compact labels; fallback to full_name, then email
  return u.username || u.full_name || u.email || null;
}

// ------------ Validators: PAN and Aadhaar -------------
function normalizePan(pan) {
  if (!pan) return null;
  const s = String(pan).toUpperCase().replace(/[^A-Z0-9]/g, '');
  return s || null;
}
function isValidPan(pan) {
  const s = normalizePan(pan);
  if (!s) return false;
  // Basic PAN pattern: 5 letters, 4 digits, 1 letter
  return /^[A-Z]{5}[0-9]{4}[A-Z]$/.test(s);
}
// Aadhaar Verhoeff validation (no hashing per requirement)
const verhoeffD = [
  [0,1,2,3,4,5,6,7,8,9],
  [1,2,3,4,0,6,7,8,9,5],
  [2,3,4,0,1,7,8,9,5,6],
  [3,4,0,1,2,8,9,5,6,7],
  [4,0,1,2,3,9,5,6,7,8],
  [5,9,8,7,6,0,4,3,2,1],
  [6,5,9,8,7,1,0,4,3,2],
  [7,6,5,9,8,2,1,0,4,3],
  [8,7,6,5,9,3,2,1,0,4],
  [9,8,7,6,5,4,3,2,1,0]
];
const verhoeffP = [
  [0,1,2,3,4,5,6,7,8,9],
  [1,5,7,6,2,8,3,0,9,4],
  [5,8,0,3,7,9,6,1,4,2],
  [8,9,1,6,0,4,3,5,2,7],
  [9,4,5,3,1,2,6,8,7,0],
  [4,2,8,6,5,7,3,9,0,1],
  [2,7,9,3,8,0,6,4,1,5],
  [7,0,4,6,9,1,3,2,5,8]
];
function isValidAadhaar(a) {
  if (!a) return false;
  const s = String(a).replace(/\s+/g, '');
  if (!/^[0-9]{12}$/.test(s)) return false;
  let c = 0;
  const arr = s.split('').map(Number).reverse();
  for (let i = 0; i < arr.length; i++) {
    const pi = verhoeffP[i % 8][arr[i]];
    c = verhoeffD[c][pi];
  }
  return c === 0;
}
function last4(s) {
  const str = String(s || '');
  return str.length >= 4 ? str.slice(-4) : str;
}

// Phone helpers: accept '+91XXXXXXXXXX' or 10-digit starting 6-9; normalize to +91XXXXXXXXXX
function normalizePhone(phone) {
  if (!phone) return null;
  const digits = String(phone).replace(/\D+/g, '');
  if (/^[6-9]\d{9}$/.test(digits)) return '+91' + digits;
  if (/^91[6-9]\d{9}$/.test(digits)) return '+' + digits;
  if (/^\+91[6-9]\d{9}$/.test(String(phone))) return String(phone);
  return null; // invalid
}

app.get('/', (req, res) => {
  res.send('Backend is working!');
});
// Health check endpoint for uptime monitors and load balancers
app.get('/healthz', async (req, res) => {
  try {
    const pool = require('./db');
    await pool.query('SELECT 1');
    res.status(200).json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});
const pool = require('./db');
// Refresh feature flags as early as possible
ensureMinimalSchema(pool)
  .then(() => refreshFeatureFlags(pool))
  .then(() => ensureUserFullProfilesView(pool))
  .then(() => {
    if (!process.env.SUPPRESS_DB_LOG) {
      console.log('[Features]', JSON.stringify(featureFlags));
    }
  })
  .catch(() => {});

// Diagnostics: feature flags visibility and refresh
app.get('/api/diagnostics/features', (req, res) => {
  res.json(featureFlags);
});
app.post('/api/diagnostics/refresh-features', async (req, res) => {
  try {
    await ensureMinimalSchema(pool);
    await refreshFeatureFlags(pool);
    if (!process.env.SUPPRESS_DB_LOG) {
      console.log('[Features refreshed]', JSON.stringify(featureFlags));
    }
    res.json(featureFlags);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ----------------------- Fuel Ops APIs -----------------------
// List storage units (optionally by type), requires auth
app.get('/api/fuel-ops/storage-units', requireAuth, async (req, res) => {
  try {
    const type = (req.query.type || '').toString().toUpperCase();
    const onlyActive = String(req.query.active || 'true').toLowerCase() !== 'false';
    const params = [];
  let sql = `SELECT id, unit_type, unit_code, capacity_liters, active
               FROM public.storage_units`;
    const where = [];
    if (type && ['TRUCK','DATUM','DISPENSER'].includes(type)) {
      params.push(type);
      where.push(`unit_type = $${params.length}`);
    }
    if (onlyActive) {
      where.push('active = TRUE');
    }
    if (where.length) {
      sql += ' WHERE ' + where.join(' AND ');
    }
    sql += ' ORDER BY unit_type, unit_code';
    const r = await pool.query(sql, params);
    res.json(r.rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Update a storage unit (vehicle/datum/dispenser) - OWNER/ADMIN
app.put('/api/fuel-ops/storage-units/:id', requireAuth, requireRole('OWNER','ADMIN'), async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (!Number.isFinite(id) || id <= 0) return res.status(400).json({ error: 'invalid id' });
    const cur = await pool.query(`SELECT * FROM public.storage_units WHERE id=$1`, [id]);
    if (!cur.rows.length) return res.status(404).json({ error: 'Not found' });
  const { unit_code, capacity_liters, vehicle_number, active } = req.body || {};
    const code = unit_code !== undefined ? String(unit_code || '').trim() : cur.rows[0].unit_code;
    const cap = capacity_liters !== undefined ? parseInt(capacity_liters,10) : cur.rows[0].capacity_liters;
    const veh = vehicle_number !== undefined ? (vehicle_number || null) : cur.rows[0].vehicle_number;
  const act = active !== undefined ? !!active : cur.rows[0].active;
    if (!code) return res.status(400).json({ error: 'unit_code required' });
    if (!Number.isFinite(cap) || cap <= 0) return res.status(400).json({ error: 'capacity_liters must be > 0' });
    // enforce unit_code uniqueness when changed
    if (code !== cur.rows[0].unit_code) {
      const exists = await pool.query(`SELECT 1 FROM public.storage_units WHERE unit_code=$1 AND id<>$2`, [code, id]);
      if (exists.rowCount) return res.status(409).json({ error: 'unit_code already exists' });
    }
    const r = await pool.query(`
      UPDATE public.storage_units
         SET unit_code=$1, capacity_liters=$2, vehicle_number=$3, active=$4, updated_at=NOW()
       WHERE id=$5
       RETURNING id, unit_type, unit_code, capacity_liters, active, vehicle_number
    `, [code, cap, veh, act, id]);
    res.json(r.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Shortcut to list dispensers only
app.get('/api/fuel-ops/dispensers', requireAuth, async (req, res) => {
  try {
  const r = await pool.query(`SELECT id, unit_type, unit_code, capacity_liters, active
                                FROM public.storage_units
                                WHERE unit_type='DISPENSER' AND active=TRUE
                                ORDER BY unit_code`);
    res.json(r.rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Drivers CRUD-lite
app.get('/api/fuel-ops/drivers', requireAuth, async (req, res) => {
  try {
    const onlyActive = String(req.query.active || 'true').toLowerCase() !== 'false';
    const r = await pool.query(`SELECT id, name, phone, driver_id, active FROM public.drivers ${onlyActive ? 'WHERE active=TRUE' : ''} ORDER BY name`);
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/api/fuel-ops/drivers', requireAuth, requireRole('OWNER','ADMIN'), async (req, res) => {
  try {
    const { name, phone, driver_id, active } = req.body || {};
    const nm = String(name || '').trim();
    const code = String(driver_id || '').trim().toUpperCase();
    if (!nm) return res.status(400).json({ error: 'name required' });
    if (!code) return res.status(400).json({ error: 'driver_id required' });
    const r = await pool.query(`
      INSERT INTO public.drivers (name, phone, driver_id, active)
      VALUES ($1,$2,$3,$4)
      ON CONFLICT (driver_id) DO NOTHING
      RETURNING id, name, phone, driver_id, active
    `, [nm, phone || null, code, active === false ? false : true]);
    if (!r.rows.length) return res.status(409).json({ error: 'driver_id already exists' });
    res.status(201).json(r.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.put('/api/fuel-ops/drivers/:id', requireAuth, requireRole('OWNER','ADMIN'), async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (!Number.isFinite(id) || id <= 0) return res.status(400).json({ error: 'invalid id' });
    const cur = await pool.query(`SELECT * FROM public.drivers WHERE id=$1`, [id]);
    if (!cur.rows.length) return res.status(404).json({ error: 'Not found' });
    const { name, phone, driver_id, active } = req.body || {};
    const nm = name !== undefined ? String(name || '').trim() : cur.rows[0].name;
    const code = driver_id !== undefined ? String(driver_id || '').trim().toUpperCase() : cur.rows[0].driver_id;
    const ph = phone !== undefined ? (phone || null) : cur.rows[0].phone;
    const act = active !== undefined ? !!active : cur.rows[0].active;
    if (!nm) return res.status(400).json({ error: 'name required' });
    if (!code) return res.status(400).json({ error: 'driver_id required' });
    if (code !== cur.rows[0].driver_id) {
      const d = await pool.query(`SELECT 1 FROM public.drivers WHERE driver_id=$1 AND id<>$2`, [code, id]);
      if (d.rowCount) return res.status(409).json({ error: 'driver_id already exists' });
    }
    const r = await pool.query(`
      UPDATE public.drivers
         SET name=$1, phone=$2, driver_id=$3, active=$4, updated_at=NOW()
       WHERE id=$5
       RETURNING id, name, phone, driver_id, active
    `, [nm, ph, code, act, id]);
    res.json(r.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Preview next lot code for a unit and date (no insert)
app.get('/api/fuel-ops/lot-code', requireAuth, async (req, res) => {
  try {
    const unitId = parseInt(req.query.unit_id, 10);
    const loadDate = req.query.load_date ? new Date(String(req.query.load_date)) : new Date();
    const liters = parseInt(req.query.loaded_liters, 10) || 0;
    if (!Number.isFinite(unitId) || unitId <= 0) return res.status(400).json({ error: 'unit_id required' });
    if (!(loadDate instanceof Date) || isNaN(loadDate.getTime())) return res.status(400).json({ error: 'load_date invalid' });
    if (!Number.isFinite(liters) || liters <= 0) return res.status(400).json({ error: 'loaded_liters must be > 0' });
    const dstr = `${loadDate.getFullYear()}-${String(loadDate.getMonth()+1).padStart(2,'0')}-${String(loadDate.getDate()).padStart(2,'0')}`;
    const r = await pool.query(`SELECT * FROM public.preview_next_lot_code($1::int, $2::date, $3::int)`, [unitId, dstr, liters]);
    if (!r.rows.length) return res.status(404).json({ error: 'Not found' });
    res.json({ lot_code: r.rows[0].lot_code, seq_index: r.rows[0].seq_index });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Create a fuel lot (inserts a row, returns lot)
app.post('/api/fuel-ops/lots', requireAuth, async (req, res) => {
  const actor = getActor(req);
  try {
  const { unit_id, load_date, loaded_liters, performed_time, load_time, tanker_code } = req.body || {};
    const unitId = parseInt(unit_id, 10);
    const liters = parseInt(loaded_liters, 10);
    if (!Number.isFinite(unitId) || unitId <= 0) return res.status(400).json({ error: 'unit_id required' });
    if (!Number.isFinite(liters) || liters <= 0) return res.status(400).json({ error: 'loaded_liters must be > 0' });
    let d = load_date ? new Date(String(load_date)) : new Date();
    if (!(d instanceof Date) || isNaN(d.getTime())) return res.status(400).json({ error: 'load_date invalid' });
    const dstr = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
    // Validate capacity before insert
    const su = await pool.query(`SELECT id, unit_code, capacity_liters FROM public.storage_units WHERE id=$1`, [unitId]);
    if (!su.rows.length) return res.status(400).json({ error: 'Unknown storage unit' });
    if (liters > su.rows[0].capacity_liters) return res.status(400).json({ error: `loaded_liters cannot exceed capacity ${su.rows[0].capacity_liters}` });
    // Use SQL function with advisory lock to create row safely
    const r = await pool.query(`SELECT * FROM public.create_fuel_lot($1::int, $2::date, $3::int)`, [unitId, dstr, liters]);
    const row = r.rows && r.rows[0];
    // If caller provided an external tanker identifier, persist it on the created lot
    try {
      if (tanker_code && row && row.id) {
        await pool.query(`UPDATE public.fuel_lots SET tanker_code = $1 WHERE id=$2`, [String(tanker_code).trim(), row.id]);
      }
    } catch (e) { if (!process.env.SUPPRESS_DB_LOG) console.warn('[warn] set tanker_code failed', e.message); }
    // Store original load time separately (do not alter created_at)
    try {
      let finalLoadTs = null;
      const hhmm = (load_time || performed_time || '').trim();
      if (/^\d{2}:\d{2}$/.test(hhmm)) {
        finalLoadTs = `${dstr} ${hhmm}:00`;
      } else if (load_time && /\d{4}-\d{2}-\d{2} \d{2}:\d{2}/.test(load_time)) {
        // Full timestamp string already
        finalLoadTs = load_time.length === 16 ? load_time+':00' : load_time;
      }
      if (finalLoadTs) {
        await pool.query(`UPDATE public.fuel_lots SET load_time = $1::timestamp WHERE id=$2`, [finalLoadTs, row.id]);
      }
    } catch {}
    // Persist actor on the created lot (best-effort)
    try {
      await pool.query(`UPDATE public.fuel_lots SET created_by=$1 WHERE id=$2`, [actor, row.id]);
    } catch {}
    // Reload with load_time field if set. Use runtime-resolved date column and expose as load_date
    let full = row;
    try {
      const dateCol = await resolveFuelLotsDateCol();
      const q2 = await pool.query(`SELECT id, unit_id, tanker_code, ${dateCol} AS load_date, tanker_capacity, loaded_liters, seq_index, seq_letters, lot_code_created, created_at, load_time, load_time_hhmm FROM public.fuel_lots WHERE id=$1`, [row.id]);
      if (q2.rows.length) full = q2.rows[0];
    } catch {}
    res.status(201).json({
      id: full.id,
      unit_id: full.unit_id,
      tanker_code: full.tanker_code,
      load_date: full.load_date,
      tanker_capacity: full.tanker_capacity,
      loaded_liters: full.loaded_liters,
      seq_index: full.seq_index,
      seq_letters: full.seq_letters,
      lot_code: full.lot_code_created,
      created_at: full.created_at,
      load_time: full.load_time || null,
      created_by: actor
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Record a lot activity (transfer/sale) and update lot counters
// Record a lot activity (transfer/sale/testing) and update lot counters
app.post('/api/fuel-ops/lots/activity', requireAuth, async (req, res) => {
  const actor = getActor(req);
  try {
    const { activity, from_unit_id, to_unit_id, to_vehicle, volume_liters, driver_id, transfer_to_empty, transfer_date, sale_date, performed_time, trip } = req.body || {};
    const act = String(activity || '').toUpperCase();
    // Allow TESTING (net-zero meter usage that should not decrement remaining lot liters)
    const allowed = new Set(['TANKER_TO_TANKER','TANKER_TO_DATUM','TANKER_TO_VEHICLE','DATUM_TO_VEHICLE','TESTING']);
    if (!allowed.has(act)) return res.status(400).json({ error: 'invalid activity' });
    const fromId = parseInt(from_unit_id, 10);
    if (!Number.isFinite(fromId) || fromId <= 0) return res.status(400).json({ error: 'from_unit_id required' });
    const toId = to_unit_id != null ? parseInt(to_unit_id, 10) : null;
    const vol = parseInt(volume_liters, 10);
    if (!Number.isFinite(vol) || vol <= 0) return res.status(400).json({ error: 'volume_liters must be > 0' });

    // Resolve driver (optional)
    let drow = null;
    if (driver_id != null) {
      const dr = await pool.query(`SELECT id, name, driver_id FROM public.drivers WHERE id=$1`, [parseInt(driver_id,10)]);
      drow = dr.rows[0] || null;
    }

    // Helper to compute cumulative inbound added liters for a lot
    async function getInboundAddedLiters(lotId) {
      // Exclude seeding transfers explicitly: flagged transfer_to_empty OR where to_lot_code_after equals the initial lot code and volume equals lot.loaded_liters
      const q = await pool.query(
        `SELECT COALESCE(SUM(fit.transfer_volume),0) AS added
           FROM public.fuel_internal_transfers fit
           JOIN public.fuel_lots fl ON fl.id = fit.to_lot_id
          WHERE fit.to_lot_id=$1
            AND NOT (
              fit.transfer_to_empty = TRUE
              OR (fit.to_lot_code_change = fl.lot_code_created AND fit.transfer_volume = fl.loaded_liters)
            )`,
        [lotId]
      );
      return Number(q.rows[0]?.added || 0);
    }
    // Helper to compute cumulative USED liters for a lot from all outbound ops (sales + internal transfers)
    async function getOutboundUsedLiters(lotId) {
      const sales = await pool.query(`SELECT COALESCE(SUM(sale_volume_liters),0) AS s FROM public.fuel_sale_transfers WHERE lot_id=$1`, [lotId]);
  const xfers = await pool.query(`SELECT COALESCE(SUM(transfer_volume),0) AS t FROM public.fuel_internal_transfers WHERE from_lot_id=$1`, [lotId]);
      return Number(sales.rows[0]?.s || 0) + Number(xfers.rows[0]?.t || 0);
    }

    // Find latest in-stock lot for source unit
    const lotQ = await pool.query(`
      SELECT * FROM public.fuel_lots
       WHERE unit_id=$1 AND stock_status='INSTOCK'
       ORDER BY created_at DESC, id DESC
       LIMIT 1
    `, [fromId]);
    if (!lotQ.rows.length) return res.status(400).json({ error: 'No in-stock lot found for source unit' });
    const lot = lotQ.rows[0];
    const lotId = lot.id;
    // Compute authoritative current state from logs: remaining = loaded + inboundAdds - outboundUsed
    const addedIn = await getInboundAddedLiters(lot.id);
    const usedOutBefore = await getOutboundUsedLiters(lot.id);
    const remaining = Math.max(0, Number(lot.loaded_liters) + addedIn - usedOutBefore);
    // For internal transfers, we validate against aggregate remaining across all lots later (FIFO split),
    // so do not block here on single-lot remaining.
    if (vol > remaining && !(act === 'TANKER_TO_TANKER' || act === 'TANKER_TO_DATUM')) {
      return res.status(400).json({ error: `insufficient volume in lot; remaining ${remaining}` });
    }

    // Fetch unit codes and metadata for from/to
    const fromUnit = await pool.query(`SELECT id, unit_code FROM public.storage_units WHERE id=$1`, [fromId]);
    if (!fromUnit.rows.length) return res.status(400).json({ error: 'Invalid from_unit_id' });
    let toUnit = { rows: [] };
    if (toId) toUnit = await pool.query(`SELECT id, unit_code, unit_type, capacity_liters FROM public.storage_units WHERE id=$1`, [toId]);

    // --- TESTING activity (net-zero; only logs testing volume and increments cumulative_testing_liters) ---
    if (act === 'TESTING') {
      // Optional performed_at timestamp logic (use transfer_date/sale_date semantics consistent with other branches)
      const dateOnly = transfer_date ? isoDateOnly(transfer_date) : (sale_date ? isoDateOnly(sale_date) : isoDateOnly(new Date()));
      let tsSql = null;
      const hhmm = (performed_time || '').trim();
      if (dateOnly && /^\d{2}:\d{2}$/.test(hhmm)) {
        tsSql = `${dateOnly} ${hhmm}:00`;
      } else if (dateOnly) {
        tsSql = `${dateOnly} 00:00:00`;
      }
      // Historically we recorded TESTING activities in `fuel_lot_activities`.
      // That table has been deprecated/removed; rely on `testing_self_transfers` and
      // `fuel_lots.cumulative_testing_liters` for audit and aggregates instead.
      let actRow = null;
      // Increment cumulative_testing_liters (best-effort). Do NOT change used_liters or stock_status.
      let updLot = null;
      try {
        const upd = await pool.query(`
          UPDATE public.fuel_lots
             SET cumulative_testing_liters = COALESCE(cumulative_testing_liters,0) + $2,
                 updated_at = NOW()
           WHERE id=$1
           RETURNING *
        `, [lot.id, vol]);
        updLot = upd.rows[0];
      } catch (e) {
        if (!process.env.SUPPRESS_DB_LOG) console.warn('[TESTING lot update warn]', e.message);
        // Return original lot if update failed
        updLot = lot;
      }
      // Also insert a record into internal transfers for audit/visibility.
      // Note: activity='TESTING' entries are excluded from stock aggregates (see helpers above),
      // so this will not affect used_liters or stock_status.
      try {
        const fromUnitCode = fromUnit.rows[0].unit_code;
        const performedAtSql = (tsSql || null);
        const tripVal = (Number.isFinite(parseInt(trip,10)) && parseInt(trip,10) > 0) ? parseInt(trip,10) : null;
        const ins = await pool.query(`
          INSERT INTO public.testing_self_transfers (
            lot_id, activity, from_unit_id, from_unit_code, to_vehicle,
            transfer_volume_liters, lot_code, driver_id, driver_name, performed_by,
            performed_at, updated_by, sale_date, trip
          ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10, COALESCE($11::timestamp, NOW()), $12, COALESCE($13::date, NULL), $14)
          RETURNING *
        `, [
          lot.id, act, fromId, fromUnitCode, fromUnitCode,
          vol, lot.lot_code_created || null, drow ? drow.id : null, drow ? drow.name : null, actor,
          performedAtSql, actor, dateOnly, tripVal
        ]);
        if (!process.env.SUPPRESS_DB_LOG) console.info('[TESTING self transfer inserted]', ins.rows[0]);
      } catch (e) {
        if (!process.env.SUPPRESS_DB_LOG) console.warn('[TESTING self insert warn]', e.message);
      }
      return res.status(201).json({ testing: actRow, lot: updLot });
    }

    // Branch to new tables
    const isInternal = act === 'TANKER_TO_TANKER' || act === 'TANKER_TO_DATUM';
    if (isInternal) {
      if (!toId) return res.status(400).json({ error: 'to_unit_id required for internal transfer' });
      // Enforce: Opening reading must be recorded for the source tanker on the transfer date; closing is NOT required here
      try {
        const dateOnly = transfer_date ? isoDateOnly(transfer_date) : isoDateOnly(new Date());
        // First check day dispenser readings
        const openQ = await pool.query(
          `SELECT opening_liters FROM public.dispenser_day_reading_logs WHERE truck_id=$1 AND reading_date=$2`,
          [fromId, dateOnly]
        );
        let hasOpening = false;
        if (openQ.rows.length && openQ.rows[0].opening_liters != null) {
          hasOpening = true;
        } else {
          // Fallback: accept trip opening (opening_liters or opening_at) created under trips for the same date
          try {
            const tripQ = await pool.query(
              `SELECT opening_at, opening_liters FROM public.truck_dispenser_trips WHERE truck_id=$1 AND reading_date=$2 AND (opening_at IS NOT NULL OR opening_liters IS NOT NULL) LIMIT 1`,
              [fromId, dateOnly]
            );
            if (tripQ.rows.length) hasOpening = true;
          } catch (e) {
            // ignore trip lookup errors
          }
        }
        if (!hasOpening) {
          return res.status(400).json({ error: 'Opening reading missing for this tanker on the selected date. Please record opening before transfers.' });
        }
      } catch (e) { /* if table missing, skip enforcement */ }
      // Find latest in-stock lot for destination unit
      let lotToQ = await pool.query(`
        SELECT * FROM public.fuel_lots
         WHERE unit_id=$1 AND stock_status='INSTOCK'
         ORDER BY created_at DESC, id DESC
         LIMIT 1
      `, [toId]);
      // If destination is a DATUM and has no lot yet, auto-create a lot seeded with this transfer's volume
      let createdNewDestLot = false;
      if (!lotToQ.rows.length) {
        const tRow = toUnit.rows[0];
        // Allow seeding for empty destination DATUM or TRUCK (tanker) using transfer volume as initial loaded_liters
        if (tRow && (tRow.unit_type === 'DATUM' || tRow.unit_type === 'TRUCK')) {
          const toUnitCap = tRow.capacity_liters;
          if (vol > toUnitCap) {
            return res.status(400).json({ error: `destination capacity exceeded: would be ${vol}/${toUnitCap}` });
          }
          // Create a new lot on the destination unit seeded with this transfer volume
          try {
            // Use the same load date as the transfer (or today if not provided)
            const createDate = transfer_date ? isoDateOnly(transfer_date) : isoDateOnly(new Date());
            const created = await pool.query(`SELECT * FROM public.create_fuel_lot($1::int, $2::date, $3::int)`, [toId, createDate, vol]);
            if (created.rows && created.rows[0]) {
              lotToQ = { rows: [created.rows[0]] };
              createdNewDestLot = true;
            }
          } catch (e) {
            if (!process.env.SUPPRESS_DB_LOG) console.warn('[WARN] failed to create destination lot for empty unit', e.message);
          }
        }
      }
      // If we created a destination lot by seeding from this transfer, mark its load_type as EMPTY_TRANSFER
      try {
        if (createdNewDestLot && lotToQ.rows[0] && lotToQ.rows[0].id) {
          await pool.query(`UPDATE public.fuel_lots SET load_type = 'EMPTY_TRANSFER' WHERE id = $1`, [lotToQ.rows[0].id]);
          // refresh lotTo with updated row
          const ref = await pool.query(`SELECT * FROM public.fuel_lots WHERE id = $1`, [lotToQ.rows[0].id]);
          if (ref.rows && ref.rows[0]) lotToQ.rows[0] = ref.rows[0];
        }
      } catch (e) {
        if (!process.env.SUPPRESS_DB_LOG) console.warn('[WARN] failed to mark created lot load_type EMPTY_TRANSFER', e.message);
      }
      // Ensure lotTo reference exists for later code
      const lotTo = (lotToQ.rows && lotToQ.rows[0]) ? lotToQ.rows[0] : null;
      if (!lotTo) return res.status(400).json({ error: 'No in-stock lot found for destination unit' });
      const sales = await pool.query(`SELECT COALESCE(SUM(sale_volume_liters),0) AS s FROM public.fuel_sale_transfers WHERE lot_id=$1`, [lotId]);
      const xfers = await pool.query(`SELECT COALESCE(SUM(transfer_volume),0) AS t FROM public.fuel_internal_transfers WHERE from_lot_id=$1 AND COALESCE(activity,'') <> 'TESTING'`, [lotId]);
      // Unit codes
      const fromUnitCode = fromUnit.rows[0].unit_code;
      const toUnitCode = (toUnit.rows[0] || {}).unit_code;

      // Collect all in-stock source lots (FIFO by creation)
      const sourceLotsQ = await pool.query(`
        SELECT * FROM public.fuel_lots
         WHERE unit_id=$1 AND stock_status='INSTOCK'
         ORDER BY created_at ASC, id ASC
      `, [fromId]);
      if (!sourceLotsQ.rows.length) return res.status(400).json({ error: 'No in-stock lot found for source unit' });
      // Compute aggregate remaining across all source lots
      const lotRemaining = [];
      let totalRemaining = 0;
      for (const L of sourceLotsQ.rows) {
        const added = await getInboundAddedLiters(L.id);
        const used = await getOutboundUsedLiters(L.id);
        const rem = Math.max(0, Number(L.loaded_liters) + added - used);
        lotRemaining.push({ lot: L, inbound: added, usedOut: used, remaining: rem });
        totalRemaining += rem;
      }
      if (vol > totalRemaining) {
        return res.status(400).json({ error: `insufficient volume in lot; remaining ${totalRemaining}` });
      }

      // Capacity guard: destination net after transfer must be <= capacity
      const toAddedBefore = createdNewDestLot ? 0 : await getInboundAddedLiters(lotTo.id);
      const toUsedOutBefore = createdNewDestLot ? 0 : await getOutboundUsedLiters(lotTo.id);
      const destCap = Number((toUnit.rows[0] || {}).capacity_liters || 0);
      if (destCap > 0) {
        const toCurrentNet = (createdNewDestLot ? 0 : (Number(lotTo.loaded_liters) + toAddedBefore - toUsedOutBefore));
        const toNetAfter = toCurrentNet + vol;
        if (toNetAfter > destCap) {
          return res.status(400).json({ error: `destination capacity exceeded: would be ${toNetAfter}/${destCap}` });
        }
      }

      // Determine timestamp/date for the transfer (allow HH:mm override)
      const dateOnly = transfer_date ? isoDateOnly(transfer_date) : null;
      let tsSql = null;
      const hhmm = (performed_time || '').trim();
      if (dateOnly && /^\d{2}:\d{2}$/.test(hhmm)) tsSql = `${dateOnly} ${hhmm}:00`;

      // Ensure destination lot load_time set when we created it via EMPTY_TRANSFER
      try { if (createdNewDestLot && tsSql) await pool.query(`UPDATE public.fuel_lots SET load_time=$1::timestamp WHERE id=$2`, [tsSql, lotTo.id]); } catch (e) { if (!process.env.SUPPRESS_DB_LOG) console.warn('[warn] set load_time for EMPTY_TRANSFER lot failed', e.message); }

      // Running dispenser adjust based on previous max
      const prevAdjQ = await pool.query(`
        SELECT COALESCE(MAX(dispenser_reading_transfer_adjust), 0)::int AS prev
          FROM public.fuel_internal_transfers
         WHERE from_unit_id = $1
      `, [fromId]);
      let runningAdjust = prevAdjQ.rows[0] ? Number(prevAdjQ.rows[0].prev) : 0;

      const xferRows = [];
      let remainingToTransfer = vol;
      for (const entry of lotRemaining) {
        if (remainingToTransfer <= 0) break;
        const take = Math.min(entry.remaining, remainingToTransfer);
        if (take <= 0) continue;

        // Compose from/to lot codes for this chunk
        const fromUsedNow = await getOutboundUsedLiters(entry.lot.id);
        const fromUsedAfter = fromUsedNow + take;
        const fromSuffix = `-${fromUsedAfter}` + (entry.inbound > 0 ? `+(${entry.inbound})` : '');
        const fromLotCodeAfter = `${entry.lot.lot_code_created}${fromSuffix}`;

        const toAddedAfter = createdNewDestLot ? 0 : (toAddedBefore + xferRows.reduce((a,r)=>a+r.transfer_volume,0) + take);
        const toSuffix = createdNewDestLot ? '' : (`-${Number(lotTo.used_liters || 0)}` + (toAddedAfter > 0 ? `+(${toAddedAfter})` : ''));
        const toLotCodeAfter = `${lotTo.lot_code_created}${toSuffix}`;

        runningAdjust += take;
        const ins = await pool.query(`
          INSERT INTO public.fuel_internal_transfers (
            from_lot_id, to_lot_id, activity,
            from_unit_id, from_unit_code, to_unit_id, to_unit_code,
            transfer_volume, from_tanker_change, from_lot_code_change, to_tanker_change, to_lot_code_change,
            transfer_to_empty, driver_name, performed_by,
            dispenser_reading_transfer_adjust, transfer_date, transfer_time
          ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16, COALESCE($17::date, CURRENT_DATE), $18::time)
          RETURNING *
        `, [
          entry.lot.id, lotTo.id, act,
          fromId, fromUnitCode, toId, toUnitCode,
          take, -take, fromLotCodeAfter, take, toLotCodeAfter,
          (createdNewDestLot ? true : !!transfer_to_empty), drow ? drow.name : null, actor,
          runningAdjust, dateOnly, (hhmm && /^\d{2}:\d{2}$/.test(hhmm) ? hhmm : '00:00')
        ]);
        xferRows.push(ins.rows[0]);

        // Update source lot used and status
        const fromNetRemaining = (Number(entry.lot.loaded_liters) + entry.inbound) - fromUsedAfter;
        const fromStock = fromNetRemaining <= 0 ? 'SOLD' : 'INSTOCK';
        await pool.query(`UPDATE public.fuel_lots SET used_liters=$1, stock_status=$2, updated_at=NOW() WHERE id=$3`, [fromUsedAfter, fromStock, entry.lot.id]);
        remainingToTransfer -= take;
      }

      // Update destination stock status (based on inbound adds)
      const toAddedAfterAll = createdNewDestLot ? vol : (toAddedBefore + xferRows.reduce((a,r)=>a + Number(r.transfer_volume||0), 0));
      const toNetRemaining = (Number(lotTo.loaded_liters) + (createdNewDestLot ? 0 : toAddedAfterAll)) - Number(lotTo.used_liters || 0);
      const toStock = toNetRemaining <= 0 ? 'SOLD' : 'INSTOCK';
      await pool.query(`UPDATE public.fuel_lots SET stock_status=$1, updated_at=NOW() WHERE id=$2`, [toStock, lotTo.id]);

      // Ensure load_type is set to EMPTY_TRANSFER for lots we seeded from an empty destination.
      try {
        if (createdNewDestLot && lotTo.id) {
          await pool.query(`UPDATE public.fuel_lots SET load_type = 'EMPTY_TRANSFER', updated_at=NOW() WHERE id = $1`, [lotTo.id]);
        }
      } catch (e) {
        if (!process.env.SUPPRESS_DB_LOG) console.warn('[WARN] failed to persist EMPTY_TRANSFER load_type on lot', e.message);
      }

      // Basic lot summary for backwards compatibility (last consumed lot)
      const last = lotRemaining.find(l => l.remaining > 0 && l.remaining >= 0) ? lotRemaining.filter(l=>l.remaining>0).slice(-1)[0] : lotRemaining[lotRemaining.length-1];
      const lastUsedNow = await getOutboundUsedLiters(last.lot.id);
      const lastSuffix = `-${lastUsedNow}` + (last.inbound>0?`+(${last.inbound})`:'');
      const lotSummary = { lot_code_initial: last.lot.lot_code_created, used_liters: lastUsedNow, loaded_liters: last.lot.loaded_liters, lot_code_by_transfer: `${last.lot.lot_code_created}${lastSuffix}` };
      return res.status(201).json({ transfers: xferRows, lot: lotSummary, total_transferred: xferRows.reduce((a,r)=>a+Number(r.transfer_volume||0),0) });
    } else {
      // Sale transfer to vehicle
      if (!to_vehicle) return res.status(400).json({ error: 'to_vehicle required' });

    const fromUnitCode = fromUnit.rows[0].unit_code;
    const inboundAdded = await getInboundAddedLiters(lot.id);

    const usedAfter = Number(lot.used_liters || 0) + vol;
    const suffix = `-${usedAfter}`;
  const lotCodeAfter = `${lot.lot_code_created}${suffix}`;

      const baseSaleDate = sale_date ? isoDateOnly(sale_date) : null;
      // For performed_at: only set when HH:mm is provided; else allow NOW() so records fall within the active trip window
      let saleDateOnly = null;
      const hhmmSale = (performed_time || '').trim();
      if (baseSaleDate && /^\d{2}:\d{2}$/.test(hhmmSale)) {
        saleDateOnly = `${baseSaleDate} ${hhmmSale}:00`;
      }
      const sale = await pool.query(`
        INSERT INTO public.fuel_sale_transfers (
          lot_id, from_unit_id, from_unit_code, to_vehicle, sale_volume_liters, lot_code_after,
          driver_id, driver_name, performed_by, activity,
          performed_at, sale_date, trip
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10, COALESCE($11::timestamp, NOW()), COALESCE($12::date, CURRENT_DATE), $13)
        RETURNING *
      `, [
        lot.id, fromId, fromUnitCode, to_vehicle, vol, lotCodeAfter,
        drow ? drow.id : null, drow ? drow.name : null, actor, act,
        saleDateOnly, sale_date ? isoDateOnly(sale_date) : null, (Number.isFinite(parseInt(trip,10)) && parseInt(trip,10) > 0) ? parseInt(trip,10) : null
      ]);

      const netRemaining = (Number(lot.loaded_liters) + inboundAdded) - usedAfter;
      const stock = netRemaining <= 0 ? 'SOLD' : 'INSTOCK';
      const upd = await pool.query(`UPDATE public.fuel_lots SET used_liters=$1, stock_status=$2, updated_at=NOW() WHERE id=$3 RETURNING *`, [usedAfter, stock, lot.id]);

      return res.status(201).json({ sale: sale.rows[0], lot: upd.rows[0] });
    }
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Mini stock summary for active TRUCK and DATUM units
// Returns per-unit capacity and current in-stock liters aggregated across ALL in-stock lots for that unit
app.get('/api/fuel-ops/stock/summary', requireAuth, async (req, res) => {
  try {
    const rows = await pool.query(`
      WITH units AS (
        SELECT id, unit_code, unit_type, capacity_liters, vehicle_number
          FROM public.storage_units
         WHERE active=TRUE AND unit_type IN ('TRUCK','DATUM')
      ),
      lots AS (
        SELECT id AS lot_id, unit_id, loaded_liters, lot_code_created, created_at
          FROM public.fuel_lots
         WHERE stock_status='INSTOCK'
      ),
      latest AS (
        SELECT DISTINCT ON (unit_id) lot_id, unit_id, lot_code_created, created_at
          FROM lots
         ORDER BY unit_id, created_at DESC, lot_id DESC
      ),
      snaps AS (
        SELECT truck_id AS unit_id, reading_at, reading_liters,
               ROW_NUMBER() OVER (PARTITION BY truck_id ORDER BY reading_at DESC) AS rn
          FROM public.truck_dispenser_meter_snapshots
      ),
      inbound AS (
        SELECT fit.to_lot_id AS lot_id,
               COALESCE(SUM(fit.transfer_volume) FILTER (
                 WHERE NOT (
                   fit.transfer_to_empty = TRUE
                   OR (fit.to_lot_code_change = fl.lot_code_created AND fit.transfer_volume = fl.loaded_liters)
                   OR (COALESCE(fit.activity,'') = 'TESTING')
                 )
               ),0)::int AS inbound_added
          FROM public.fuel_internal_transfers fit
          JOIN public.fuel_lots fl ON fl.id = fit.to_lot_id
         GROUP BY fit.to_lot_id
      ),
      -- Aggregate outbound since latest snapshot (or all-time if no snapshot) at unit level
      sale_unit AS (
        SELECT fst.from_unit_id AS unit_id,
               COALESCE(SUM(
                 CASE
                   WHEN sn.reading_at IS NOT NULL THEN CASE WHEN COALESCE(fst.performed_at, fst.sale_date::timestamp) >= sn.reading_at THEN fst.sale_volume_liters ELSE 0 END
                   ELSE fst.sale_volume_liters
                 END
               ),0)::int AS sale_out_since,
               MAX(COALESCE(fst.performed_at, fst.sale_date::timestamp)) AS last_sale_at
          FROM public.fuel_sale_transfers fst
          LEFT JOIN (SELECT unit_id, reading_at FROM snaps WHERE rn=1) sn ON sn.unit_id = fst.from_unit_id
         GROUP BY fst.from_unit_id
      ),

      sales AS (
        SELECT lot_id, COALESCE(SUM(sale_volume_liters),0)::int AS sale_only
          FROM public.fuel_sale_transfers
         GROUP BY lot_id
      ),
      xfer_unit AS (
        SELECT fit.from_unit_id AS unit_id,
               COALESCE(SUM(
                 CASE
                   WHEN sn.reading_at IS NOT NULL THEN CASE WHEN (fit.transfer_date::timestamp + fit.transfer_time) >= sn.reading_at THEN fit.transfer_volume ELSE 0 END
                   ELSE fit.transfer_volume
                 END
               ),0)::int AS xfer_out_since,
               MAX(fit.transfer_date::timestamp + fit.transfer_time) AS last_xfer_at
          FROM public.fuel_internal_transfers fit
          LEFT JOIN (SELECT unit_id, reading_at FROM snaps WHERE rn=1) sn ON sn.unit_id = fit.from_unit_id
         WHERE COALESCE(fit.activity,'') <> 'TESTING'
         GROUP BY fit.from_unit_id
      ),
      testing_unit AS (
        SELECT tst.from_unit_id AS unit_id,
               COALESCE(SUM(
                 CASE
                   WHEN sn.reading_at IS NOT NULL THEN CASE WHEN COALESCE(tst.performed_at, tst.sale_date::timestamp) >= sn.reading_at THEN tst.transfer_volume_liters ELSE 0 END
                   ELSE tst.transfer_volume_liters
                 END
               ),0)::int AS test_out_since,
               MAX(COALESCE(tst.performed_at, tst.sale_date::timestamp)) AS last_test_at
          FROM public.testing_self_transfers tst
          LEFT JOIN (SELECT unit_id, reading_at FROM snaps WHERE rn=1) sn ON sn.unit_id = tst.from_unit_id
         GROUP BY tst.from_unit_id
      ),
      outbound_x AS (
        SELECT from_lot_id AS lot_id, COALESCE(SUM(transfer_volume),0)::int AS outbound_transfers
          FROM public.fuel_internal_transfers
         WHERE COALESCE(activity,'') <> 'TESTING'
         GROUP BY from_lot_id
      ),
      per_lot AS (
        SELECT l.unit_id, l.lot_id, l.lot_code_created, l.created_at,
               COALESCE((SELECT fl.loaded_liters FROM public.fuel_lots fl WHERE fl.id=l.lot_id),0) AS loaded_liters,
               GREATEST(0,
                 COALESCE((SELECT fl.loaded_liters FROM public.fuel_lots fl WHERE fl.id=l.lot_id),0)
                 + COALESCE(i.inbound_added,0)
                 - (COALESCE(o.outbound_transfers,0) + COALESCE(s.sale_only,0))
               )::int AS remaining
          FROM lots l
          LEFT JOIN inbound i ON i.lot_id = l.lot_id
          LEFT JOIN sales s ON s.lot_id = l.lot_id
          LEFT JOIN outbound_x o ON o.lot_id = l.lot_id
      ),
      agg AS (
        SELECT unit_id, COALESCE(SUM(remaining),0)::int AS instock_liters
          FROM per_lot
         GROUP BY unit_id
      )
  SELECT u.id, u.unit_code, u.unit_type, u.capacity_liters, u.vehicle_number,
             lt.lot_id, lt.lot_code_created,
             COALESCE(a.instock_liters,0)::int AS instock_liters,
             COALESCE(s.sale_only,0)::int AS sale_only_liters,
             COALESCE(sn.reading_liters, NULL) AS latest_snapshot_liters,
             COALESCE(sn.reading_at, NULL) AS latest_snapshot_at,
               COALESCE(su.sale_out_since,0)::int AS sale_out_since,
               COALESCE(xu.xfer_out_since,0)::int AS xfer_out_since,
               COALESCE(tu.test_out_since,0)::int AS test_out_since,
               GREATEST(COALESCE(su.last_sale_at, '1970-01-01'::timestamp), COALESCE(xu.last_xfer_at, '1970-01-01'::timestamp), COALESCE(tu.last_test_at, '1970-01-01'::timestamp)) AS last_outbound_at,
             su.last_sale_at
        FROM units u
        LEFT JOIN latest lt ON lt.unit_id = u.id
        LEFT JOIN sales s ON s.lot_id = lt.lot_id
        LEFT JOIN agg a ON a.unit_id = u.id
        LEFT JOIN snaps sn ON sn.unit_id = u.id AND sn.rn = 1
        LEFT JOIN sale_unit su ON su.unit_id = u.id
        LEFT JOIN xfer_unit xu ON xu.unit_id = u.id
        LEFT JOIN testing_unit tu ON tu.unit_id = u.id
       ORDER BY u.unit_type, u.unit_code
    `);
    const items = rows.rows.map(r => ({
      id: r.id,
      unit_code: r.unit_code,
      unit_type: r.unit_type,
      capacity_liters: Number(r.capacity_liters || 0),
      vehicle_number: r.vehicle_number || null,
  lot_id: r.lot_id || null,
  lot_code_initial: r.lot_code_created || null,
      instock_liters: Number(r.instock_liters || 0),
      sale_only_liters: Number(r.sale_only_liters || 0),
      meter_reading_liters: (() => {
        const snap = r.latest_snapshot_liters != null ? Number(r.latest_snapshot_liters) : null;
        // Outbound since latest snapshot (or all-time if no snapshot)
        const outSince = Number(r.sale_out_since || 0) + Number(r.xfer_out_since || 0) + Number(r.test_out_since || 0);
        if (snap == null) return outSince;
        return snap + outSince;
      })(),
      latest_snapshot_liters: r.latest_snapshot_liters != null ? Number(r.latest_snapshot_liters) : null,
      latest_snapshot_at: r.latest_snapshot_at || null,
      last_sale_at: r.last_sale_at || null,
      last_outbound_at: r.last_outbound_at || null
    }));
    res.json({ items, generatedAt: new Date().toISOString() });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Create a storage unit (tanker). Restricted to OWNER/ADMIN.
app.post('/api/fuel-ops/storage-units', requireAuth, requireRole('OWNER','ADMIN'), async (req, res) => {
  try {
    const { unit_code, capacity_liters, unit_type, vehicle_number } = req.body || {};
    const rawType = String(unit_type || 'TRUCK').toUpperCase();
    // Accept 'STORAGE' from UI and normalize to 'DATUM'
    const type = rawType === 'STORAGE' ? 'DATUM' : rawType;
    if (!['TRUCK','DATUM','DISPENSER'].includes(type)) return res.status(400).json({ error: 'unit_type invalid' });
    const code = (unit_code || '').toString().trim();
    const cap = parseInt(capacity_liters, 10);
    if (!code) return res.status(400).json({ error: 'unit_code required' });
    if (!Number.isFinite(cap) || cap <= 0) return res.status(400).json({ error: 'capacity_liters must be > 0' });
    const r = await pool.query(
      `INSERT INTO public.storage_units (unit_type, unit_code, capacity_liters, active, vehicle_number)
       VALUES ($1,$2,$3,TRUE,$4)
       ON CONFLICT (unit_code) DO NOTHING
       RETURNING id, unit_type, unit_code, capacity_liters, active, vehicle_number`,
      [type, code, cap, vehicle_number || null]
    );
    if (!r.rows.length) return res.status(409).json({ error: 'unit_code already exists' });
    res.status(201).json(r.rows[0]);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Vehicles list shortcut (TRUCK/DATUM)
app.get('/api/fuel-ops/vehicles', requireAuth, async (req, res) => {
  try {
    const type = (req.query.type || '').toString().toUpperCase();
    if (!['TRUCK','DATUM'].includes(type)) return res.status(400).json({ error: 'type must be TRUCK or DATUM' });
  const r = await pool.query(`SELECT id, unit_type, unit_code, vehicle_number, capacity_liters, active
                                 FROM public.storage_units
                                 WHERE unit_type=$1 AND active=TRUE
                                 ORDER BY unit_code`, [type]);
    res.json(r.rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// --- Opening suggestions & daily upsert for dispenser and odometer (by truck) ---
function isoDateOnly(s) {
  const d = new Date(String(s));
  if (isNaN(d.getTime())) return null;
  const y = d.getFullYear(); const m = String(d.getMonth()+1).padStart(2,'0'); const day = String(d.getDate()).padStart(2,'0');
  return `${y}-${m}-${day}`;
}

// Format a JS Date to SQL timestamp string in local time (no timezone conversion)
function toSqlLocalTs(dt) {
  if (!dt) return null;
  const d = new Date(dt);
  if (isNaN(d.getTime())) return null;
  const y = d.getFullYear();
  const m = String(d.getMonth()+1).padStart(2,'0');
  const day = String(d.getDate()).padStart(2,'0');
  const hh = String(d.getHours()).padStart(2,'0');
  const mm = String(d.getMinutes()).padStart(2,'0');
  const ss = String(d.getSeconds()).padStart(2,'0');
  return `${y}-${m}-${day} ${hh}:${mm}:${ss}`;
}

// Get opening suggestion for dispenser liters
app.get('/api/fuel-ops/opening-suggestion/dispenser', requireAuth, async (req, res) => {
  try {
    const truckId = parseInt(req.query.truck_id, 10);
    const dateStr = isoDateOnly(req.query.date || new Date());
    if (!Number.isFinite(truckId) || truckId <= 0) return res.status(400).json({ error: 'truck_id required' });
    // yesterday closing (use day reading logs)
    const r = await pool.query(
      `SELECT closing_liters, reading_date FROM public.dispenser_day_reading_logs
        WHERE truck_id=$1 AND reading_date < $2::date
        ORDER BY reading_date DESC LIMIT 1`, [truckId, dateStr]
    );
    if (r.rows.length) return res.json({ opening: r.rows[0].closing_liters, source: 'yesterday', date: r.rows[0].reading_date });
    // If none, return null (requires manual opening)
    res.json({ opening: null, source: 'first' });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Get opening suggestion for odometer km
app.get('/api/fuel-ops/opening-suggestion/odometer', requireAuth, async (req, res) => {
  try {
    const truckId = parseInt(req.query.truck_id, 10);
    const dateStr = isoDateOnly(req.query.date || new Date());
    if (!Number.isFinite(truckId) || truckId <= 0) return res.status(400).json({ error: 'truck_id required' });
    const r = await pool.query(
      `SELECT closing_km, reading_date FROM public.truck_odometer_day_readings
        WHERE truck_id=$1 AND reading_date < $2::date
        ORDER BY reading_date DESC LIMIT 1`, [truckId, dateStr]
    );
    if (r.rows.length) return res.json({ opening: r.rows[0].closing_km, source: 'yesterday', date: r.rows[0].reading_date });
    res.json({ opening: null, source: 'first' });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Get existing daily dispenser record (proxy to day reading logs)
app.get('/api/fuel-ops/day/dispenser', requireAuth, async (req, res) => {
  try {
    const truckId = parseInt(req.query.truck_id, 10);
    const dateStr = isoDateOnly(req.query.date || new Date());
    const r = await pool.query(`SELECT * FROM public.dispenser_day_reading_logs WHERE truck_id=$1 AND reading_date=$2`, [truckId, dateStr]);
    res.json(r.rows[0] || null);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// List all daily dispenser readings for a truck (ascending by date) — use day reading logs
app.get('/api/fuel-ops/day/dispenser/list', requireAuth, async (req, res) => {
  try {
    const truckId = parseInt(req.query.truck_id, 10);
    if (!Number.isFinite(truckId) || truckId <= 0) return res.status(400).json({ error: 'truck_id required' });
    const r = await pool.query(`SELECT id, truck_id, reading_date, opening_liters, closing_liters, opening_at, closing_at, note, driver_name, driver_code, created_by, created_by_user_id, created_at, updated_at FROM public.dispenser_day_reading_logs WHERE truck_id=$1 ORDER BY reading_date ASC`, [truckId]);
    res.json({ items: r.rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Trips CRUD-lite: list/create/edit for multiple trips per day
app.get('/api/fuel-ops/trips', requireAuth, async (req, res) => {
  try {
    const truckId = parseInt(req.query.truck_id, 10);
    const dateStr = isoDateOnly(req.query.date || new Date());
  if (!Number.isFinite(truckId) || truckId <= 0) return res.status(400).json({ error: 'truck_id required' });
  if (!dateStr) return res.status(400).json({ error: 'date invalid' });
    const r = await pool.query(
      `SELECT id, truck_id, reading_date, trip_no, opening_liters, closing_liters, opening_at, closing_at, note, driver_name, driver_code
         FROM public.truck_dispenser_trips
        WHERE truck_id=$1 AND reading_date=$2
        ORDER BY trip_no ASC`,
      [truckId, dateStr]
    );
    res.json({ items: r.rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/fuel-ops/trips', requireAuth, async (req, res) => {
  try {
    const { truck_id, date, opening_liters, opening_at, note, driver_name, driver_code } = req.body || {};
    const truckId = parseInt(truck_id, 10);
    const dateStr = isoDateOnly(date || new Date());
    if (!Number.isFinite(truckId) || truckId <= 0) return res.status(400).json({ error: 'truck_id required' });
    if (!dateStr) return res.status(400).json({ error: 'date invalid' });
    // Determine next trip number for the given truck and date
    const nextQ = await pool.query(
      `SELECT COALESCE(MAX(trip_no),0)+1 AS next
         FROM public.truck_dispenser_trips
        WHERE truck_id=$1 AND reading_date=$2`,
      [truckId, dateStr]
    );
    const nextNo = Number(nextQ.rows[0]?.next || 1);
    // Parse opening_at timestamp if provided
    let openingTsSql = null;
    if (opening_at && isValidDateTimeString(opening_at)) {
      // Preserve local time exactly as provided (avoid UTC conversion)
      const s = String(opening_at).replace('T',' ').slice(0,19);
      // Try to normalize via local formatter; if parse fails, keep string
      const d = new Date(String(s));
      openingTsSql = !isNaN(d.getTime()) ? toSqlLocalTs(d) : s;
    }
    const r = await pool.query(
      `INSERT INTO public.truck_dispenser_trips (truck_id, reading_date, trip_no, opening_liters, opening_at, note, driver_name, driver_code, created_by, created_by_user_id)
       VALUES ($1,$2,$3,COALESCE($4,0),$5,$6,$7,$8,$9,$10)
       RETURNING *`,
  [truckId, dateStr, nextNo, (opening_liters!=null? parseInt(opening_liters,10): null), openingTsSql, note || null, driver_name || null, driver_code || null, getActor(req), req.user?.sub || null]
    );
    res.status(201).json(r.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.patch('/api/fuel-ops/trips/:id', requireAuth, async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (!Number.isFinite(id) || id <= 0) return res.status(400).json({ error: 'invalid id' });
    const { opening_liters, closing_liters, opening_at, closing_at, note, driver_name, driver_code } = req.body || {};
    const parts = [];
    const vals = [];
    let idx = 1;
    if (opening_liters != null) { parts.push(`opening_liters=$${idx++}`); vals.push(parseInt(opening_liters,10)); }
    if (closing_liters != null) { parts.push(`closing_liters=$${idx++}`); vals.push(parseInt(closing_liters,10)); }
    if (opening_at) {
      const s = String(opening_at).replace('T',' ').slice(0,19);
      const d = new Date(String(s));
      parts.push(`opening_at=$${idx++}`);
      vals.push(!isNaN(d.getTime()) ? toSqlLocalTs(d) : s);
    }
    if (closing_at) {
      const s = String(closing_at).replace('T',' ').slice(0,19);
      const d = new Date(String(s));
      parts.push(`closing_at=$${idx++}`);
      vals.push(!isNaN(d.getTime()) ? toSqlLocalTs(d) : s);
    }
    if (note !== undefined) { parts.push(`note=$${idx++}`); vals.push(note || null); }
    // Accept driver fields so UI can persist driver selection on trip updates
    if (driver_name !== undefined) { parts.push(`driver_name=$${idx++}`); vals.push(driver_name || null); }
    if (driver_code !== undefined) { parts.push(`driver_code=$${idx++}`); vals.push(driver_code || null); }
    if (!parts.length) return res.status(400).json({ error: 'no fields to update' });
    parts.push(`updated_at=NOW()`);
    vals.push(id);
    const r = await pool.query(`UPDATE public.truck_dispenser_trips SET ${parts.join(', ')} WHERE id=$${idx} RETURNING *`, vals);
    if (!r.rows.length) return res.status(404).json({ error: 'not found' });
    res.json(r.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Delete a trip. Safety: allow deletion ONLY for the last trip of the day to avoid gaps.
app.delete('/api/fuel-ops/trips/:id', requireAuth, async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (!Number.isFinite(id) || id <= 0) return res.status(400).json({ error: 'invalid id' });
    const q = await pool.query(`SELECT id, truck_id, reading_date, trip_no FROM public.truck_dispenser_trips WHERE id=$1`, [id]);
    if (!q.rows.length) return res.status(404).json({ error: 'not found' });
    const row = q.rows[0];
    const m = await pool.query(`SELECT MAX(trip_no) AS max_no FROM public.truck_dispenser_trips WHERE truck_id=$1 AND reading_date=$2`, [row.truck_id, row.reading_date]);
    const maxNo = Number(m.rows[0]?.max_no || 0);
    if (row.trip_no !== maxNo) return res.status(400).json({ error: 'only the last trip for the day can be deleted' });
    await pool.query(`DELETE FROM public.truck_dispenser_trips WHERE id=$1`, [id]);
    res.json({ ok: true, deleted_id: id });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
// Upsert daily dispenser record
// Upsert daily dispenser record (migrated to use dispenser_day_reading_logs)
// This endpoint will create a day log entry and also create opening/closing meter snapshots.
app.post('/api/fuel-ops/day/dispenser', requireAuth, async (req, res) => {
  try {
    const { truck_id, date, opening_liters, closing_liters, opening_at, closing_at, note, driver_name, driver_code } = req.body || {};
    const truckId = parseInt(truck_id, 10);
    const dateStr = isoDateOnly(date || new Date());
    const open = Number(opening_liters);
    const close = (closing_liters == null) ? null : Number(closing_liters);
    if (!Number.isFinite(truckId) || truckId <= 0) return res.status(400).json({ error: 'truck_id required' });
    if (!Number.isFinite(open) || open < 0) return res.status(400).json({ error: 'opening_liters invalid' });
    if (close != null && (!Number.isFinite(close) || close < open)) return res.status(400).json({ error: 'closing_liters must be >= opening' });
    // Reject if record for same date exists (create-only)
    const exists = await pool.query(`SELECT 1 FROM public.dispenser_day_reading_logs WHERE truck_id=$1 AND reading_date=$2`, [truckId, dateStr]);
    if (exists.rowCount > 0) {
      const [y,m,d] = dateStr.split('-');
      return res.status(409).json({ error: `readings are submitted for ${d}/${m}/${y}. to edit go to edit button.` });
    }
    // Normalize timestamps
    let openingTs = null, closingTs = null;
    if (opening_at && isValidDateTimeString(opening_at)) openingTs = new Date(String(opening_at));
    if (closing_at && isValidDateTimeString(closing_at)) closingTs = new Date(String(closing_at));
    if (!openingTs) openingTs = new Date(`${dateStr}T00:00:00`);
    if (!closingTs && close != null) closingTs = new Date(`${dateStr}T23:59:59`);
    const openingSql = openingTs ? toSqlLocalTs(openingTs) : `${dateStr} 00:00:00`;
    const closingSql = closingTs ? toSqlLocalTs(closingTs) : null;
    const su = await pool.query(`SELECT unit_code FROM public.storage_units WHERE id=$1`, [truckId]);
    const truckCode = su.rows.length ? su.rows[0].unit_code : null;
    const r = await pool.query(
      `INSERT INTO public.dispenser_day_reading_logs (truck_id, truck_code, reading_date, opening_liters, closing_liters, opening_at, closing_at, note, driver_name, driver_code, created_by, created_by_user_id)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
       RETURNING *`,
      [truckId, truckCode, dateStr, open, close, openingSql, closingSql, note || null, driver_name || null, driver_code || null, getActor(req), req.user?.sub || null]
    );
    // Also create opening/closing snapshots if closing provided
    try {
      await pool.query(`INSERT INTO public.truck_dispenser_meter_snapshots (truck_id, reading_at, reading_liters, source, note, created_by, created_by_user_id) VALUES ($1,$2,$3,'OPENING',$4,$5,$6)`, [truckId, openingSql, open, 'Opening snapshot', getActor(req), req.user?.sub || null]);
      if (closingSql) await pool.query(`INSERT INTO public.truck_dispenser_meter_snapshots (truck_id, reading_at, reading_liters, source, note, created_by, created_by_user_id) VALUES ($1,$2,$3,'CLOSING',$4,$5,$6)`, [truckId, closingSql, close, 'Closing snapshot', getActor(req), req.user?.sub || null]);
    } catch (e) { if (!process.env.SUPPRESS_DB_LOG) console.warn('[snapshots insert warn]', e.message); }
    res.status(201).json(r.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// New: Day reading logs CRUD for dispenser_day_reading_logs
app.get('/api/fuel-ops/day/logs', requireAuth, async (req, res) => {
  try {
    const truckId = parseInt(req.query.truck_id, 10);
    const dateStr = isoDateOnly(req.query.date || new Date());
    if (!Number.isFinite(truckId) || truckId <= 0) return res.status(400).json({ error: 'truck_id required' });
    if (!dateStr) return res.status(400).json({ error: 'invalid date' });
    const r = await pool.query(`SELECT * FROM public.dispenser_day_reading_logs WHERE truck_id=$1 AND reading_date=$2`, [truckId, dateStr]);
    res.json(r.rows[0] || null);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/fuel-ops/day/logs/list', requireAuth, async (req, res) => {
  try {
    const truckId = parseInt(req.query.truck_id, 10);
    if (!Number.isFinite(truckId) || truckId <= 0) return res.status(400).json({ error: 'truck_id required' });
    const r = await pool.query(`SELECT id, truck_id, reading_date, opening_liters, closing_liters, opening_at, closing_at, note, driver_name, driver_code, created_by, created_by_user_id, created_at, updated_at FROM public.dispenser_day_reading_logs WHERE truck_id=$1 ORDER BY reading_date ASC`, [truckId]);
    res.json({ items: r.rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/fuel-ops/day/logs', requireAuth, async (req, res) => {
  try {
    const { truck_id, date, opening_liters, closing_liters, opening_at, closing_at, note, driver_name, driver_code } = req.body || {};
    const truckId = parseInt(truck_id, 10);
    const dateStr = isoDateOnly(date || new Date());
    const open = Number(opening_liters);
    const close = (closing_liters == null) ? null : Number(closing_liters);
    if (!Number.isFinite(truckId) || truckId <= 0) return res.status(400).json({ error: 'truck_id required' });
    if (!Number.isFinite(open) || open < 0) return res.status(400).json({ error: 'opening_liters invalid' });
    if (close != null && (!Number.isFinite(close) || close < open)) return res.status(400).json({ error: 'closing_liters must be >= opening' });
    // Reject if record for same date exists (create-only)
    const exists = await pool.query(`SELECT 1 FROM public.dispenser_day_reading_logs WHERE truck_id=$1 AND reading_date=$2`, [truckId, dateStr]);
    if (exists.rowCount > 0) return res.status(409).json({ error: 'readings already submitted for this date' });
    // Normalize timestamps: coerce user-entered values to local SQL timestamp strings
    let openingSql = null;
    let closingSql = null;
    // Resolve truck_code for easier lookups and to store denormalized code
    let truckCode = null;
    try {
      const su = await pool.query(`SELECT unit_code FROM public.storage_units WHERE id=$1`, [truckId]);
      truckCode = su.rows.length ? su.rows[0].unit_code : null;
    } catch (e) { if (!process.env.SUPPRESS_DB_LOG) console.warn('[day/logs truck_code lookup warn]', e.message); }
    if (opening_at) openingSql = coerceLocalSqlTimestamp(String(opening_at));
    if (!openingSql) openingSql = `${dateStr} 00:00:00`;
    if (closing_at) closingSql = coerceLocalSqlTimestamp(String(closing_at));
    // closingSql may remain null if no closing provided
    const r = await pool.query(
      `INSERT INTO public.dispenser_day_reading_logs (truck_id, truck_code, reading_date, opening_liters, closing_liters, opening_at, closing_at, note, driver_name, driver_code, created_by, created_by_user_id)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
       RETURNING *`,
      [truckId, truckCode, dateStr, open, close, openingSql, closingSql, note || null, driver_name || null, driver_code || null, getActor(req), req.user?.sub || null]
    );
    res.status(201).json(r.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.patch('/api/fuel-ops/day/logs/:id', requireAuth, async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (!Number.isFinite(id) || id <= 0) return res.status(400).json({ error: 'invalid id' });
    const { opening_liters, closing_liters, opening_at, closing_at, note } = req.body || {};
    const parts = [];
    const vals = [];
    let idx = 1;
    if (opening_liters != null) { parts.push(`opening_liters=$${idx++}`); vals.push(parseInt(opening_liters,10)); }
    if (closing_liters != null) { parts.push(`closing_liters=$${idx++}`); vals.push(parseInt(closing_liters,10)); }
    if (opening_at) {
      const coerced = coerceLocalSqlTimestamp(String(opening_at));
      parts.push(`opening_at=$${idx++}`);
      vals.push(coerced || String(opening_at).replace('T',' ').slice(0,19));
    }
    if (closing_at) {
      const coerced = coerceLocalSqlTimestamp(String(closing_at));
      parts.push(`closing_at=$${idx++}`);
      vals.push(coerced || String(closing_at).replace('T',' ').slice(0,19));
    }
    if (note !== undefined) { parts.push(`note=$${idx++}`); vals.push(note || null); }
    if (!parts.length) return res.status(400).json({ error: 'no fields to update' });
    parts.push(`updated_at=NOW()`);
    vals.push(id);
    const r = await pool.query(`UPDATE public.dispenser_day_reading_logs SET ${parts.join(', ')} WHERE id=$${idx} RETURNING *`, vals);
    if (!r.rows.length) return res.status(404).json({ error: 'not found' });
    res.json(r.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Delete a day reading log by id
app.delete('/api/fuel-ops/day/logs/:id', requireAuth, async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (!Number.isFinite(id) || id <= 0) return res.status(400).json({ error: 'invalid id' });
    const del = await pool.query(`DELETE FROM public.dispenser_day_reading_logs WHERE id=$1 RETURNING id, truck_id`, [id]);
    if (!del.rows.length) return res.status(404).json({ error: 'not found' });
    res.json({ ok: true, deleted_id: del.rows[0].id, truck_id: del.rows[0].truck_id });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Edit daily dispenser record (allow updating closing liters, times, testing, note, driver fields)
// Edit daily dispenser record (migrated to operate on dispenser_day_reading_logs)
app.patch('/api/fuel-ops/day/dispenser', requireAuth, async (req, res) => {
  try {
    const { truck_id, date, opening_liters, closing_liters, opening_at, closing_at, note, driver_name, driver_code } = req.body || {};
    const truckId = parseInt(truck_id, 10);
    const dateStr = isoDateOnly(date || new Date());
    if (!Number.isFinite(truckId) || truckId <= 0) return res.status(400).json({ error: 'truck_id required' });
    const existingQ = await pool.query(`SELECT * FROM public.dispenser_day_reading_logs WHERE truck_id=$1 AND reading_date=$2`, [truckId, dateStr]);
    if (!existingQ.rows.length) return res.status(404).json({ error: 'day reading not found' });
    const existing = existingQ.rows[0];
    const open = opening_liters != null ? Number(opening_liters) : Number(existing.opening_liters);
    const close = closing_liters != null ? Number(closing_liters) : Number(existing.closing_liters);
    if (!Number.isFinite(open) || open < 0) return res.status(400).json({ error: 'opening_liters invalid' });
    if (close != null && (!Number.isFinite(close) || close < open)) return res.status(400).json({ error: 'closing_liters must be >= opening' });
    // Parse/normalize timestamps (optional updates)
    let openingTs = existing.opening_at ? new Date(existing.opening_at) : new Date(`${dateStr}T00:00:00`);
    let closingTs = existing.closing_at ? new Date(existing.closing_at) : (existing.closing_liters != null ? new Date(`${dateStr}T23:59:59`) : null);
    if (opening_at && isValidDateTimeString(opening_at)) openingTs = new Date(String(opening_at));
    if (closing_at && isValidDateTimeString(closing_at)) closingTs = new Date(String(closing_at));
    if (isNaN(openingTs.getTime()) || (closingTs && isNaN(closingTs.getTime()))) return res.status(400).json({ error: 'opening_at/closing_at invalid' });
    const parts = [];
    const vals = [];
    let idx = 1;
    parts.push(`opening_liters=$${idx++}`); vals.push(open);
    parts.push(`closing_liters=$${idx++}`); vals.push(close != null ? close : null);
    parts.push(`opening_at=$${idx++}`); vals.push(toSqlLocalTs(openingTs));
    parts.push(`closing_at=$${idx++}`); vals.push(closingTs ? toSqlLocalTs(closingTs) : null);
    parts.push(`note=$${idx++}`); vals.push(note != null ? note : existing.note);
    parts.push(`driver_name=$${idx++}`); vals.push(driver_name != null ? driver_name : existing.driver_name);
    parts.push(`driver_code=$${idx++}`); vals.push(driver_code != null ? driver_code : existing.driver_code);
    parts.push(`updated_at=NOW()`);
    vals.push(truckId); vals.push(dateStr);
    const upd = await pool.query(`UPDATE public.dispenser_day_reading_logs SET ${parts.join(', ')} WHERE truck_id=$${idx++} AND reading_date=$${idx} RETURNING *`, vals);
    if (!upd.rows.length) return res.status(404).json({ error: 'not found' });
    // Optionally create adjustment snapshots when opening/closing changed
    try {
      const changedOpening = open !== Number(existing.opening_liters);
      const changedClosing = (close != null && close !== Number(existing.closing_liters));
      if (changedOpening) {
        await pool.query(`INSERT INTO public.truck_dispenser_meter_snapshots (truck_id, reading_at, reading_liters, source, note, created_by, created_by_user_id) VALUES ($1,$2,$3,'OPENING_EDIT',$4,$5,$6)`, [truckId, toSqlLocalTs(openingTs), open, 'Edited opening liters', getActor(req), req.user?.sub || null]);
      }
      if (changedClosing) {
        await pool.query(`INSERT INTO public.truck_dispenser_meter_snapshots (truck_id, reading_at, reading_liters, source, note, created_by, created_by_user_id) VALUES ($1,$2,$3,'CLOSING_EDIT',$4,$5,$6)`, [truckId, toSqlLocalTs(closingTs), close, 'Edited closing liters', getActor(req), req.user?.sub || null]);
      }
    } catch (e) { if (!process.env.SUPPRESS_DB_LOG) console.warn('[edit snapshots warn]', e.message); }
    res.json(upd.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Truck dispenser meter snapshots: create & list
app.post('/api/fuel-ops/meter-snapshots', requireAuth, async (req, res) => {
  try {
    const { truck_id, reading_liters, reading_at, note } = req.body || {};
    const tid = parseInt(truck_id, 10);
    const val = Number(reading_liters);
    if (!Number.isFinite(tid) || tid <= 0) return res.status(400).json({ error: 'truck_id required' });
    if (!Number.isFinite(val) || val < 0) return res.status(400).json({ error: 'reading_liters must be >= 0' });
    // Preserve user-entered local date/time exactly as a wall-clock timestamp (no UTC shift)
    let tsSql = null;
    if (reading_at) {
      if (!isValidDateTimeString(String(reading_at))) return res.status(400).json({ error: 'reading_at invalid' });
      tsSql = coerceLocalSqlTimestamp(String(reading_at));
      if (!tsSql) return res.status(400).json({ error: 'reading_at invalid' });
    } else {
      tsSql = fmtSqlTsLocal(new Date());
    }
    const su = await pool.query(`SELECT id, unit_type FROM public.storage_units WHERE id=$1`, [tid]);
    if (!su.rows.length) return res.status(400).json({ error: 'Unknown storage unit' });
    if (!['TRUCK','DATUM'].includes(su.rows[0].unit_type)) {
      return res.status(400).json({ error: 'Unsupported unit type for meter snapshot' });
    }
    const r = await pool.query(`
      INSERT INTO public.truck_dispenser_meter_snapshots (truck_id, reading_at, reading_liters, source, note, created_by, created_by_user_id)
      VALUES ($1,$2,$3,'SNAPSHOT',$4,$5,$6)
      RETURNING id, truck_id, reading_at, reading_liters, source, note, created_at
    `, [tid, tsSql, val, note || null, getActor(req), req.user?.sub || null]);
    res.status(201).json(r.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/api/fuel-ops/meter-snapshots', requireAuth, async (req, res) => {
  try {
    const tid = parseInt(req.query.truck_id, 10);
    const fromStr = req.query.from ? String(req.query.from) : null;
    const toStr = req.query.to ? String(req.query.to) : null;
    const limit = Math.max(1, Math.min(1000, parseInt(req.query.limit || '200', 10) || 200));
    if (!Number.isFinite(tid) || tid <= 0) return res.status(400).json({ error: 'truck_id required' });
    const params = [tid];
    let where = ' WHERE truck_id = $1';
    if (fromStr && isValidDateTimeString(fromStr)) {
      const fSql = coerceLocalSqlTimestamp(fromStr);
      if (fSql) { params.push(fSql); where += ` AND reading_at >= $${params.length}`; }
    }
    if (toStr && isValidDateTimeString(toStr)) {
      const tSql = coerceLocalSqlTimestamp(toStr);
      if (tSql) { params.push(tSql); where += ` AND reading_at <= $${params.length}`; }
    }
    const sql = `SELECT id, truck_id, reading_at, reading_liters, source, note, created_at
                   FROM public.truck_dispenser_meter_snapshots
                   ${where}
                   ORDER BY reading_at DESC
                   LIMIT ${limit}`;
    const r = await pool.query(sql, params);
    res.json({ items: r.rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Daily reconciliation for a truck and date
app.get('/api/fuel-ops/reconcile/daily', requireAuth, async (req, res) => {
  try {
    const truckId = parseInt(req.query.truck_id, 10);
    const dateStr = isoDateOnly(req.query.date || new Date());
    if (!Number.isFinite(truckId) || truckId <= 0) return res.status(400).json({ error: 'truck_id required' });
    if (!dateStr) return res.status(400).json({ error: 'date invalid' });

    // We'll prefer sources in this order:
    // 1) dispenser_day_reading_logs (authoritative operator-entered logs)
    // 2) truck_dispenser_meter_snapshots (preferred local snapshot near day bounds)
    // 3) trip-level readings (earliest opening_at and latest closing_at recorded for trips on that date)
    // 4) fallback to day bounds (00:00:00 / 23:59:59)

    // Use dispenser_day_reading_logs as the authoritative source for opening/closing readings.
    // The legacy `truck_dispenser_day_readings` table is no longer required for reconciliation.
    let O = 0, C = 0; // opening and closing meter liters
    let openSQL = null, closeSQL = null; // SQL-local timestamp window strings
    let meterDeltaAvailable = false;

    let Lrow = null;
    try {
      const logsQ = await pool.query(`SELECT * FROM public.dispenser_day_reading_logs WHERE truck_id=$1 AND reading_date=$2`, [truckId, dateStr]);
      if (logsQ.rows.length) {
        Lrow = logsQ.rows[0];
        O = Number(Lrow.opening_liters || 0);
        C = Number(Lrow.closing_liters || 0);
        meterDeltaAvailable = (Lrow.opening_liters != null && Lrow.closing_liters != null);
        openSQL = Lrow.opening_at ? toSqlLocalTs(Lrow.opening_at) : `${dateStr} 00:00:00`;
        closeSQL = Lrow.closing_at ? toSqlLocalTs(Lrow.closing_at) : `${dateStr} 23:59:59`;
      }
    } catch (e) {
      if (!process.env.SUPPRESS_DB_LOG) console.warn('[reconcile logs lookup warn]', e.message);
      // leave O/C as 0 and fall through to day bounds fallback below
    }

    // Helper day bounds
    const dayStart = `${dateStr} 00:00:00`;
    const dayEnd = `${dateStr} 23:59:59`;

    // If logs didn't provide timestamps/values, we'll fall back to day bounds below and leave
    // meterDeltaAvailable=false which signals the UI that meter-derived delta is unavailable.

    // Final fallback: if openSQL/closeSQL still missing, use day bounds
    if (!openSQL) openSQL = dayStart;
    if (!closeSQL) closeSQL = dayEnd;
    // Sales within window
    const salesQ = await pool.query(`
      SELECT COALESCE(SUM(sale_volume_liters),0)::int AS s
        FROM public.fuel_sale_transfers
       WHERE from_unit_id=$1 AND performed_at >= $2::timestamp AND performed_at <= $3::timestamp
    `, [truckId, openSQL, closeSQL]);
    const S = Number(salesQ.rows[0]?.s || 0);
    // Internal transfers out/in
    const toutQ = await pool.query(`
      SELECT COALESCE(SUM(transfer_volume),0)::int AS t
        FROM public.fuel_internal_transfers
       WHERE from_unit_id=$1 AND COALESCE(activity,'') <> 'TESTING' AND (transfer_date::timestamp + transfer_time) >= $2::timestamp AND (transfer_date::timestamp + transfer_time) <= $3::timestamp
    `, [truckId, openSQL, closeSQL]);
    const tinQ = await pool.query(`
      SELECT COALESCE(SUM(transfer_volume),0)::int AS t
        FROM public.fuel_internal_transfers
       WHERE to_unit_id=$1 AND COALESCE(activity,'') <> 'TESTING' AND (transfer_date::timestamp + transfer_time) >= $2::timestamp AND (transfer_date::timestamp + transfer_time) <= $3::timestamp
    `, [truckId, openSQL, closeSQL]);
    const T_out = Number(toutQ.rows[0]?.t || 0);
  const T_in = Number(tinQ.rows[0]?.t || 0);
    // Include any testing transfers logged as internal transfers for this truck in the same window
    const testingTransfersQ = await pool.query(
      `SELECT COALESCE((SELECT SUM(transfer_volume_liters) FROM public.testing_self_transfers WHERE from_unit_id=$1 AND performed_at >= $2::timestamp AND performed_at <= $3::timestamp),0) AS t`,
      [truckId, openSQL, closeSQL]
    );
    const T_test = (Lrow ? Number(Lrow.testing_used_liters || 0) : 0) + Number(testingTransfersQ.rows[0]?.t || 0);
    const deltaM = meterDeltaAvailable ? Number((C - O).toFixed(3)) : null;
    // Dispenser meters only increase on outflow (sales, transfers out, testing). Transfer-in does not affect the meter.
    const deltaE = Number((S + T_out + T_test).toFixed(3));
    const delta = (deltaM == null) ? null : Number((deltaM - deltaE).toFixed(3));
    // Human-readable note about discrepancy
    let note = null;
    if (delta == null) {
      note = 'Meter delta unavailable (no day reading or insufficient snapshots)';
    } else if (delta > 0) {
      note = `Meter reading is more by ${Math.abs(delta)} than transfers and sales`;
    } else if (delta < 0) {
      note = `Meter reading is less by ${Math.abs(delta)} than transfers and sales`;
    } else {
      note = 'Meter matches transfers and sales';
    }
    res.json({
      truck_id: truckId,
      date: dateStr,
      opening: O,
      opening_at: openSQL,
      closing: C,
      closing_at: closeSQL,
      sales: S,
      transfers_out: T_out,
      transfers_in: T_in,
      testing_used_liters: T_test,
      delta_meter: deltaM,
      delta_expected: deltaE,
      delta_difference: delta,
      note
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Off-hours summary between two timestamps for a truck
app.get('/api/fuel-ops/offhours', requireAuth, async (req, res) => {
  try {
    const truckId = parseInt(req.query.truck_id, 10);
    const fromStr = req.query.from;
    const toStr = req.query.to;
    if (!Number.isFinite(truckId) || truckId <= 0) return res.status(400).json({ error: 'truck_id required' });
    if (!isValidDateTimeString(fromStr) || !isValidDateTimeString(toStr)) return res.status(400).json({ error: 'from/to invalid' });
    const fromSQL = new Date(String(fromStr)).toISOString().replace('T',' ').slice(0,19);
    const toSQL = new Date(String(toStr)).toISOString().replace('T',' ').slice(0,19);
    // Meter delta from snapshots if both ends exist; else null
    const startSnap = await pool.query(`SELECT reading_liters FROM public.truck_dispenser_meter_snapshots WHERE truck_id=$1 AND reading_at <= $2 ORDER BY reading_at DESC LIMIT 1`, [truckId, fromSQL]);
    const endSnap = await pool.query(`SELECT reading_liters FROM public.truck_dispenser_meter_snapshots WHERE truck_id=$1 AND reading_at >= $2 ORDER BY reading_at ASC LIMIT 1`, [truckId, toSQL]);
    const meterDelta = (startSnap.rows.length && endSnap.rows.length) ? Number(endSnap.rows[0].reading_liters) - Number(startSnap.rows[0].reading_liters) : null;
  const transfersOutQ = await pool.query(`SELECT COALESCE(SUM(transfer_volume),0)::int AS t FROM public.fuel_internal_transfers WHERE from_unit_id=$1 AND COALESCE(activity,'') <> 'TESTING' AND (transfer_date::timestamp + transfer_time) >= $2 AND (transfer_date::timestamp + transfer_time) <= $3`, [truckId, fromSQL, toSQL]);
  const transfersInQ = await pool.query(`SELECT COALESCE(SUM(transfer_volume),0)::int AS t FROM public.fuel_internal_transfers WHERE to_unit_id=$1 AND COALESCE(activity,'') <> 'TESTING' AND (transfer_date::timestamp + transfer_time) >= $2 AND (transfer_date::timestamp + transfer_time) <= $3`, [truckId, fromSQL, toSQL]);
    const salesQ = await pool.query(`SELECT COALESCE(SUM(sale_volume_liters),0)::int AS s FROM public.fuel_sale_transfers WHERE from_unit_id=$1 AND performed_at >= $2 AND performed_at <= $3`, [truckId, fromSQL, toSQL]);
    const T_out = Number(transfersOutQ.rows[0]?.t || 0);
  const T_in = Number(transfersInQ.rows[0]?.t || 0);
    const S = Number(salesQ.rows[0]?.s || 0);
  // Count testing transfers in the off-hours expected meter delta (meters advance on outflow)
  const testingOffQ = await pool.query(
    `SELECT COALESCE((SELECT SUM(transfer_volume_liters) FROM public.testing_self_transfers WHERE from_unit_id=$1 AND performed_at >= $2 AND performed_at <= $3),0) AS t`,
    [truckId, fromSQL, toSQL]
  );
  const T_test_off = Number(testingOffQ.rows[0]?.t || 0);
  const expected = S + T_out + T_test_off;
    const residual = (meterDelta == null) ? null : Number((meterDelta - expected).toFixed(3));
    res.json({
      truck_id: truckId,
      from: fromSQL,
      to: toSQL,
      meter_delta: meterDelta,
      sales: S,
      transfers_out: T_out,
      transfers_in: T_in,
      expected_delta: expected,
      residual
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Get existing daily odometer record
app.get('/api/fuel-ops/day/odometer', requireAuth, async (req, res) => {
  try {
    const truckId = parseInt(req.query.truck_id, 10);
    const dateStr = isoDateOnly(req.query.date || new Date());
    const r = await pool.query(`SELECT * FROM public.truck_odometer_day_readings WHERE truck_id=$1 AND reading_date=$2`, [truckId, dateStr]);
    res.json(r.rows[0] || null);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
// Upsert daily odometer record
app.post('/api/fuel-ops/day/odometer', requireAuth, async (req, res) => {
  try {
    const { truck_id, date, opening_km, closing_km, note, driver_name, driver_code, opening_time, closing_time, opening_at, closing_at } = req.body || {};
    const truckId = parseInt(truck_id, 10);
    const dateStr = isoDateOnly(date || new Date());
    const open = Number(opening_km);
    const close = Number(closing_km);
    if (!Number.isFinite(truckId) || truckId <= 0) return res.status(400).json({ error: 'truck_id required' });
    if (!Number.isFinite(open) || open < 0) return res.status(400).json({ error: 'opening_km invalid' });
    if (!Number.isFinite(close) || close < open) return res.status(400).json({ error: 'closing_km must be >= opening' });
    const exists = await pool.query(`SELECT 1 FROM public.truck_odometer_day_readings WHERE truck_id=$1 AND reading_date=$2`, [truckId, dateStr]);
    if (exists.rowCount > 0) {
      const [y,m,d] = dateStr.split('-');
      return res.status(409).json({ error: `readings are submitted for ${d}/${m}/${y}. to edit go to edit button.` });
    }
    // derive opening_at/closing_at from HH:mm or full timestamp
    function buildTs(hhmm, overrideTs) {
      try {
        if (overrideTs) return new Date(overrideTs);
        const t = (hhmm || '').toString().trim();
        if (!t) return null;
        const [hh, mm] = t.split(':');
        if (hh == null || mm == null) return null;
        return new Date(`${dateStr}T${String(hh).padStart(2,'0')}:${String(mm).padStart(2,'0')}:00`);
      } catch { return null; }
    }
    const openingAtTs = buildTs(opening_time, opening_at);
    const closingAtTs = buildTs(closing_time, closing_at);
    const r = await pool.query(
      `INSERT INTO public.truck_odometer_day_readings (truck_id, reading_date, opening_km, closing_km, note, driver_name, driver_code, opening_at, closing_at, created_by, created_by_user_id)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
       RETURNING *`,
      [truckId, dateStr, open, close, note || null, driver_name || null, driver_code || null, openingAtTs, closingAtTs, getActor(req), req.user?.sub || null]
    );
    res.status(201).json(r.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Edit daily odometer record
app.patch('/api/fuel-ops/day/odometer', requireAuth, async (req, res) => {
  try {
    const { truck_id, date, opening_km, closing_km, note, driver_name, driver_code, opening_time, closing_time, opening_at, closing_at } = req.body || {};
    const truckId = parseInt(truck_id, 10);
    const dateStr = isoDateOnly(date || new Date());
    if (!Number.isFinite(truckId) || truckId <= 0) return res.status(400).json({ error: 'truck_id required' });
    const existingQ = await pool.query(`SELECT * FROM public.truck_odometer_day_readings WHERE truck_id=$1 AND reading_date=$2`, [truckId, dateStr]);
    if (!existingQ.rows.length) return res.status(404).json({ error: 'day reading not found' });
    const existing = existingQ.rows[0];
    const open = opening_km != null ? Number(opening_km) : Number(existing.opening_km);
    const close = closing_km != null ? Number(closing_km) : Number(existing.closing_km);
    if (!Number.isFinite(open) || open < 0) return res.status(400).json({ error: 'opening_km invalid' });
    if (!Number.isFinite(close) || close < open) return res.status(400).json({ error: 'closing_km must be >= opening' });
    function buildTs(hhmm, overrideTs, fallback) {
      try {
        if (overrideTs != null) return new Date(overrideTs);
        const t = (hhmm || '').toString().trim();
        if (!t) return fallback;
        const [hh, mm] = t.split(':');
        if (hh == null || mm == null) return fallback;
        return new Date(`${dateStr}T${String(hh).padStart(2,'0')}:${String(mm).padStart(2,'0')}:00`);
      } catch { return fallback; }
    }
    const openingAtTs = buildTs(opening_time, opening_at, existing.opening_at || null);
    const closingAtTs = buildTs(closing_time, closing_at, existing.closing_at || null);
    const upd = await pool.query(`
      UPDATE public.truck_odometer_day_readings
         SET opening_km=$3,
             closing_km=$4,
             note=$5,
             driver_name=$6,
             driver_code=$7,
             opening_at=$8,
             closing_at=$9,
             updated_at=NOW()
       WHERE truck_id=$1 AND reading_date=$2
       RETURNING *
    `, [truckId, dateStr, open, close, note != null ? note : existing.note, driver_name != null ? driver_name : existing.driver_name, driver_code != null ? driver_code : existing.driver_code, openingAtTs, closingAtTs]);
    res.json(upd.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// List daily odometer records for a truck (descending by date)
app.get('/api/fuel-ops/day/odometer/list', requireAuth, async (req, res) => {
  try {
    const truckId = parseInt(req.query.truck_id, 10);
    const limit = Math.max(1, Math.min(365, parseInt(req.query.limit || '90', 10) || 90));
    if (!Number.isFinite(truckId) || truckId <= 0) return res.status(400).json({ error: 'truck_id required' });
    const r = await pool.query(`SELECT id, truck_id, reading_date, opening_km, closing_km, opening_at, closing_at, note, driver_name, driver_code, created_at, updated_at
                                  FROM public.truck_odometer_day_readings
                                 WHERE truck_id=$1
                                 ORDER BY reading_date DESC
                                 LIMIT $2`, [truckId, limit]);
    res.json({ items: r.rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Delete a daily odometer record (by id or truck/date)
app.delete('/api/fuel-ops/day/odometer', requireAuth, async (req, res) => {
  try {
    const idRaw = req.query.id;
    if (idRaw) {
      const id = parseInt(idRaw, 10);
      if (!Number.isFinite(id) || id <= 0) return res.status(400).json({ error: 'invalid id' });
      const del = await pool.query(`DELETE FROM public.truck_odometer_day_readings WHERE id=$1 RETURNING id`, [id]);
      if (!del.rows.length) return res.status(404).json({ error: 'not found' });
      return res.json({ ok: true, deleted_id: id });
    }
    const truckId = parseInt(req.query.truck_id, 10);
    const dateStr = isoDateOnly(req.query.date || new Date());
    if (!Number.isFinite(truckId) || truckId <= 0) return res.status(400).json({ error: 'truck_id required' });
    const del = await pool.query(`DELETE FROM public.truck_odometer_day_readings WHERE truck_id=$1 AND reading_date=$2 RETURNING id`, [truckId, dateStr]);
    if (!del.rows.length) return res.status(404).json({ error: 'not found' });
    res.json({ ok: true, deleted_id: del.rows[0].id });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// =========================
// Listing endpoints for Loads subtabs
// =========================

// Determine fuel_lots date column name at runtime (supports load_date vs loaded_date)
let FUEL_LOTS_DATE_COL = null;
async function resolveFuelLotsDateCol() {
  if (FUEL_LOTS_DATE_COL) return FUEL_LOTS_DATE_COL;
  try {
    const q = await pool.query(
      `SELECT column_name FROM information_schema.columns
         WHERE table_schema='public' AND table_name='fuel_lots'
           AND column_name IN ('load_date','loaded_date')
         ORDER BY CASE column_name WHEN 'load_date' THEN 1 ELSE 2 END LIMIT 1`
    );
    FUEL_LOTS_DATE_COL = (q.rows[0] && q.rows[0].column_name) || 'loaded_date';
  } catch { FUEL_LOTS_DATE_COL = 'loaded_date'; }
  return FUEL_LOTS_DATE_COL;
}

// Recent lots for a unit (tanker or datum)
app.get('/api/fuel-ops/lots/list', requireAuth, async (req, res) => {
  try {
    const dateCol = await resolveFuelLotsDateCol();
    const unitIdRaw = req.query.unit_id;
    const unitId = unitIdRaw != null ? parseInt(unitIdRaw, 10) : null;
    const limit = Math.max(1, Math.min(500, parseInt(req.query.limit || '50', 10) || 50));
    const loadType = (req.query.load_type || '').toString().toUpperCase(); // PURCHASE | EMPTY_TRANSFER
    const unitType = (req.query.unit_type || '').toString().toUpperCase(); // TRUCK | DATUM
    // When unit_id omitted, return recent lots across all TRUCK/DATUM units (active only)
    let params = [];
    let sqlBase = `FROM public.fuel_lots fl
                   JOIN public.storage_units su ON su.id = fl.unit_id
                  WHERE su.active=TRUE`;
    if (unitType && ['TRUCK','DATUM'].includes(unitType)) {
      params.push(unitType);
      sqlBase += ` AND su.unit_type = $${params.length}`;
    } else {
      sqlBase += ` AND su.unit_type IN ('TRUCK','DATUM')`;
    }
    if (loadType && ['PURCHASE','EMPTY_TRANSFER'].includes(loadType)) {
      params.push(loadType);
      sqlBase += ` AND fl.load_type = $${params.length}`;
    }
    // Extended computed columns for purchase display:
    // remaining_liters: clamp to 0 when SOLD else loaded - used (no inbound adds considered here; UI wants purchase perspective)
    // transfer_volume_liters: when SOLD and used > loaded (indicates tanker-to-tanker transfers increasing used counter beyond initial load)
    // transfer_to_unit_codes: list of distinct destination tanker codes this lot transferred to
  // Use the detected date column and expose as load_date for the UI.
  const selectCols = `SELECT fl.id, fl.unit_id,
                 fl.${dateCol} AS load_date,
                 fl.loaded_liters, fl.used_liters, fl.stock_status,
                 fl.lot_code_created AS lot_code_initial, fl.created_at, fl.load_time, fl.load_type, su.unit_code, su.unit_type,
                               CASE WHEN fl.stock_status='SOLD' THEN 0 ELSE GREATEST(0, fl.loaded_liters - fl.used_liters) END::int AS remaining_liters,
                               (
                                 SELECT COALESCE(SUM(fit.transfer_volume) FILTER (WHERE COALESCE(fit.activity,'') <> 'TESTING'),0)::int
                                   FROM public.fuel_internal_transfers fit
                                  WHERE fit.to_lot_id = fl.id
                               ) AS transfer_volume_liters,
                               (
                                 SELECT string_agg(DISTINCT fit.to_unit_code, ',')
                                   FROM public.fuel_internal_transfers fit
                                 WHERE fit.from_lot_id = fl.id AND fit.to_unit_code IS NOT NULL
                               ) AS transfer_to_unit_codes`;
    if (Number.isFinite(unitId) && unitId > 0) {
      // Reset params for clarity per-branch
      const p = [unitId];
      let where = ' WHERE fl.unit_id=$1';
      if (loadType && ['PURCHASE','EMPTY_TRANSFER'].includes(loadType)) {
        p.push(loadType); where += ` AND fl.load_type = $${p.length}`;
      }
  const sql = `${selectCols}
       FROM public.fuel_lots fl
       JOIN public.storage_units su ON su.id = fl.unit_id
       ${where}
       ORDER BY COALESCE(fl.load_time, fl.created_at) DESC, fl.id DESC
       LIMIT ${limit}`;
      const r = await pool.query(sql, p);
      return res.json({ items: r.rows });
    }
    // All units path
  const sql = `${selectCols}
         ${sqlBase}
         ORDER BY COALESCE(fl.load_time, fl.created_at) DESC, fl.id DESC
         LIMIT ${limit}`;
    const r = await pool.query(sql, params);
    return res.json({ items: r.rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Internal transfers for a unit (as source or destination)
// NOTE: Removed Internal Transfers listing endpoint as per requirement

// Sale transfers for a source unit
app.get('/api/fuel-ops/transfers/sales', requireAuth, async (req, res) => {
  try {
    const unitId = parseInt(req.query.unit_id, 10);
    const limit = Math.max(1, Math.min(500, parseInt(req.query.limit || '50', 10) || 50));
    if (!Number.isFinite(unitId) || unitId <= 0) return res.status(400).json({ error: 'unit_id required' });
    const uq = await pool.query(`SELECT unit_code FROM public.storage_units WHERE id=$1`, [unitId]);
    const unitCode = (uq.rows[0] && uq.rows[0].unit_code) || null;
    const params = [unitId, limit, unitCode];
    // Combine canonical sales with TESTING internal transfers (mapped) so TESTING filled-back-to-same appears in lists
    const r = await pool.query(
        `SELECT * FROM (
           SELECT fst.id, fst.lot_id, fst.from_unit_id, fst.from_unit_code, fst.to_vehicle,
                  fst.sale_volume_liters AS volume_liters,
                  fl.lot_code_created AS lot_code_initial,
                  fst.lot_code_after AS lot_code_by_transfer,
                  fst.driver_name, fst.performed_at, fst.activity, fst.created_at, fst.trip
             FROM public.fuel_sale_transfers fst
             LEFT JOIN public.fuel_lots fl ON fl.id = fst.lot_id
            WHERE (fst.from_unit_id=$1 OR ($3 IS NOT NULL AND fst.from_unit_code=$3))
          UNION ALL
           SELECT tst.id, tst.lot_id, tst.from_unit_id, tst.from_unit_code,
               tst.to_vehicle AS to_vehicle,
               tst.transfer_volume_liters::int AS volume_liters,
               fl.lot_code_created AS lot_code_initial,
               tst.lot_code AS lot_code_by_transfer,
               tst.driver_name, tst.performed_at AS performed_at, tst.activity, tst.performed_at AS created_at, tst.trip::int AS trip
             FROM public.testing_self_transfers tst
             LEFT JOIN public.fuel_lots fl ON fl.id = tst.lot_id
            WHERE (tst.from_unit_id=$1 OR ($3 IS NOT NULL AND tst.from_unit_code=$3))
         ) t
         ORDER BY COALESCE(t.performed_at, t.created_at) DESC, t.id DESC
         LIMIT $2`,
      params
    );
    res.json({ items: r.rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Simple sales transfers list (no filtering) for display
app.get('/api/fuel-ops/transfers/sales/list', requireAuth, async (req, res) => {
  try {
    const limit = Math.max(1, Math.min(1000, parseInt(req.query.limit || '100', 10) || 100));
    // Combine canonical sales with TESTING activities and LOADED lots for display purposes
    const r = await pool.query(
      `
      SELECT * FROM (
        SELECT id, from_unit_code, to_vehicle, performed_at, sale_date, sale_volume_liters, lot_code_after, driver_name, performed_by, activity, trip
          FROM public.fuel_sale_transfers
        UNION ALL
        SELECT tst.id AS id,
               COALESCE(su.unit_code, '') AS from_unit_code,
               tst.to_vehicle AS to_vehicle,
               tst.performed_at AS performed_at,
               tst.sale_date AS sale_date,
               tst.transfer_volume_liters AS sale_volume_liters,
               tst.lot_code AS lot_code_after,
               tst.driver_name AS driver_name,
               tst.performed_by AS performed_by,
               tst.activity AS activity,
               tst.trip::int AS trip
          FROM public.testing_self_transfers tst
          LEFT JOIN public.storage_units su ON su.id = tst.from_unit_id
        UNION ALL
        SELECT fl.id AS id,
               COALESCE(fl.tanker_code, '') AS from_unit_code,
               NULL::text AS to_vehicle,
               COALESCE(fl.load_time, fl.created_at) AS performed_at,
               COALESCE((fl.load_time::date), (fl.created_at::date)) AS sale_date,
               fl.loaded_liters AS sale_volume_liters,
               fl.lot_code_created AS lot_code_after,
               NULL::text AS driver_name,
               NULL::text AS performed_by,
               'LOADED'::text AS activity,
               NULL::int AS trip
          FROM public.fuel_lots fl
          JOIN public.storage_units su ON su.id = fl.unit_id
      ) t
      ORDER BY COALESCE(t.sale_date, (t.performed_at::date)) DESC, t.id DESC
      LIMIT $1
      `,
      [limit]
    );
    res.json({ items: r.rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Simple list of internal transfers (no complex filters) for display purposes
app.get('/api/fuel-ops/transfers/internal/list', requireAuth, async (req, res) => {
  try {
    const limit = Math.max(1, Math.min(1000, parseInt(req.query.limit || '100', 10) || 100));
    const r = await pool.query(
      `SELECT id,
              from_unit_code,
              to_unit_code,
              transfer_date,
              transfer_time,
              transfer_volume,
              from_lot_code_change,
              to_lot_code_change,
              transfer_to_empty,
              driver_name,
              performed_by,
              activity
         FROM public.fuel_internal_transfers
        ORDER BY transfer_date DESC, transfer_time DESC, id DESC
        LIMIT $1`,
      [limit]
    );
    res.json({ items: r.rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Edit an internal transfer record (volume and performed time only)
app.patch('/api/fuel-ops/transfers/internal/:id', requireAuth, async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (!Number.isFinite(id) || id <= 0) return res.status(400).json({ error: 'invalid id' });
  const { transfer_volume_liters, transfer_volume, performed_time } = req.body || {};
    const existingQ = await pool.query(`SELECT * FROM public.fuel_internal_transfers WHERE id=$1`, [id]);
    if (!existingQ.rows.length) return res.status(404).json({ error: 'not found' });
    const existing = existingQ.rows[0];
    let vol = existing.transfer_volume || existing.transfer_volume_liters;
    const volInput = transfer_volume != null ? transfer_volume : transfer_volume_liters;
    if (volInput != null) {
      const vNum = Number(volInput);
      if (!Number.isFinite(vNum) || vNum <= 0) return res.status(400).json({ error: 'transfer_volume_liters invalid' });
      vol = vNum;
    }
    let newTime = null;
    if (performed_time) {
      const t = String(performed_time).trim();
      const [hh, mm] = t.split(':');
      if (hh != null && mm != null) {
        newTime = `${String(hh).padStart(2,'0')}:${String(mm).padStart(2,'0')}:00`;
      }
    }
    const upd = await pool.query(`
      UPDATE public.fuel_internal_transfers
         SET transfer_volume=$2,
             transfer_time=COALESCE($3::time, transfer_time),
             updated_at=NOW()
       WHERE id=$1
       RETURNING *
    `, [id, vol, newTime]);
    res.json(upd.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Edit a testing_self_transfers record (allow editing volume and performed_at/time)
app.patch('/api/fuel-ops/transfers/testing/:id', requireAuth, async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (!Number.isFinite(id) || id <= 0) return res.status(400).json({ error: 'invalid id' });
    const { transfer_volume_liters, transfer_volume, performed_time, sale_date } = req.body || {};
    const existingQ = await pool.query(`SELECT * FROM public.testing_self_transfers WHERE id=$1`, [id]);
    if (!existingQ.rows.length) return res.status(404).json({ error: 'not found' });
    const existing = existingQ.rows[0];
    const parts = [];
    const vals = [];
    let idx = 1;
    if (transfer_volume != null) { parts.push(`transfer_volume_liters=$${idx++}`); vals.push(parseInt(transfer_volume,10)); }
    else if (transfer_volume_liters != null) { parts.push(`transfer_volume_liters=$${idx++}`); vals.push(parseInt(transfer_volume_liters,10)); }
    // Time edit (HH:mm). Determine date base: provided sale_date -> provided, else existing.sale_date -> existing.performed_at date -> today
    if (performed_time != null) {
      const hhmm = String(performed_time).trim();
      if (/^\d{2}:\d{2}$/.test(hhmm)) {
        const baseDate = sale_date ? isoDateOnly(sale_date)
          : (existing.sale_date ? isoDateOnly(existing.sale_date) : (existing.performed_at ? isoDateOnly(existing.performed_at) : isoDateOnly(new Date())));
        if (baseDate) {
          parts.push(`performed_at=$${idx++}`);
          vals.push(`${baseDate} ${hhmm}:00`);
        }
      }
    }
    if (sale_date != null && performed_time == null) {
      // allow updating sale_date alone (date portion of performed_at)
      const baseDate = isoDateOnly(sale_date);
      if (baseDate) {
        // derive hh:mm from existing.performed_at if present, else default to 00:00
        const curDate = existing.performed_at ? isoDateOnly(existing.performed_at) : baseDate;
        const timePart = existing.performed_at ? String(existing.performed_at).slice(11,19) : '00:00:00';
        parts.push(`performed_at=$${idx++}`);
        vals.push(`${baseDate} ${timePart}`);
        parts.push(`sale_date=$${idx++}`);
        vals.push(baseDate);
      }
    }
    if (!parts.length) return res.status(400).json({ error: 'no fields to update' });
    parts.push(`updated_at=NOW()`);
    vals.push(id);
    const q = await pool.query(`UPDATE public.testing_self_transfers SET ${parts.join(', ')} WHERE id=$${idx} RETURNING *`, vals);
    if (!q.rows.length) return res.status(404).json({ error: 'not found' });
    res.json(q.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Delete a testing_self_transfers record
app.delete('/api/fuel-ops/transfers/testing/:id', requireAuth, async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (!Number.isFinite(id) || id <= 0) return res.status(400).json({ error: 'invalid id' });
    const del = await pool.query(`DELETE FROM public.testing_self_transfers WHERE id=$1 RETURNING *`, [id]);
    if (!del.rows.length) return res.status(404).json({ error: 'not found' });
    res.json({ deleted: true, row: del.rows[0] });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Consolidated per-day operations for a truck (sales, transfers in/out, loads, totals, remaining liters)
app.get('/api/fuel-ops/ops/day', requireAuth, async (req, res) => {
  try {
    const truckId = parseInt(req.query.truck_id, 10);
    const dateStrRaw = req.query.date || new Date();
    const dateStr = isoDateOnly(dateStrRaw);
    if (!Number.isFinite(truckId) || truckId <= 0) return res.status(400).json({ error: 'truck_id required' });
    if (!dateStr) return res.status(400).json({ error: 'invalid date' });

    // Helper queries to compute aggregate inbound/outbound usage for remaining liters
    async function getInboundAddedLiters(lotId) {
      const q = await pool.query(
        `SELECT COALESCE(SUM(transfer_volume),0) AS added
           FROM public.fuel_internal_transfers
          WHERE to_lot_id=$1`, [lotId]
      );
      return Number(q.rows[0]?.added || 0);
    }
    async function getOutboundUsedLiters(lotId) {
      const sales = await pool.query(`SELECT COALESCE(SUM(sale_volume_liters),0) AS s FROM public.fuel_sale_transfers WHERE lot_id=$1`, [lotId]);
  const xfers = await pool.query(`SELECT COALESCE(SUM(transfer_volume),0) AS t FROM public.fuel_internal_transfers WHERE from_lot_id=$1`, [lotId]);
      return Number(sales.rows[0]?.s || 0) + Number(xfers.rows[0]?.t || 0);
    }

    // Current in-stock lot for truck (most recent)
    const lotQ = await pool.query(`SELECT * FROM public.fuel_lots WHERE unit_id=$1 AND stock_status='INSTOCK' ORDER BY created_at DESC, id DESC LIMIT 1`, [truckId]);
    let lotInfo = null; let remainingLiters = null;
    if (lotQ.rows.length) {
      const lot = lotQ.rows[0];
      const inbound = await getInboundAddedLiters(lot.id);
      const outbound = await getOutboundUsedLiters(lot.id);
      remainingLiters = Math.max(0, Number(lot.loaded_liters) + inbound - outbound);
      // Clamp remaining to unit capacity if defined to avoid showing > capacity in UI
      try {
        const su = await pool.query(`SELECT capacity_liters FROM public.storage_units WHERE id=$1`, [truckId]);
        const cap = Number(su.rows[0] && su.rows[0].capacity_liters ? su.rows[0].capacity_liters : 0);
        if (cap > 0 && Number.isFinite(remainingLiters)) {
          remainingLiters = Math.min(remainingLiters, cap);
        }
      } catch (e) { if (!process.env.SUPPRESS_DB_LOG) console.warn('[ops/day clamp warn]', e.message); }
      lotInfo = {
        id: lot.id,
        lot_code_initial: lot.lot_code_created,
        loaded_liters: lot.loaded_liters,
        used_liters: lot.used_liters,
        inbound_adds_liters: inbound,
        outbound_used_liters: outbound,
        remaining_liters: remainingLiters
      };
    }

    // Day-filtered operations
    const salesQ = await pool.query(
      `SELECT id, from_unit_id, from_unit_code, to_vehicle, sale_volume_liters, lot_code_after, driver_name, performed_at, sale_date, activity
         FROM public.fuel_sale_transfers
        WHERE from_unit_id=$1 AND COALESCE(sale_date, performed_at::date) = $2::date
        ORDER BY COALESCE(performed_at, sale_date) ASC, id ASC`,
      [truckId, dateStr]
    );
    const transfersOutQ = await pool.query(
      `SELECT id, from_unit_id, from_unit_code, to_unit_id, to_unit_code, transfer_volume, from_lot_code_change, to_lot_code_change, transfer_to_empty, driver_name, transfer_date, transfer_time, activity
         FROM public.fuel_internal_transfers
        WHERE from_unit_id=$1 AND transfer_date = $2::date
        ORDER BY transfer_date ASC, transfer_time ASC, id ASC`,
      [truckId, dateStr]
    );
    const transfersInQ = await pool.query(
      `SELECT id, from_unit_id, from_unit_code, to_unit_id, to_unit_code, transfer_volume, from_lot_code_change, to_lot_code_change, transfer_to_empty, driver_name, transfer_date, transfer_time, activity
         FROM public.fuel_internal_transfers
        WHERE to_unit_id=$1 AND transfer_date = $2::date
        ORDER BY transfer_date ASC, transfer_time ASC, id ASC`,
      [truckId, dateStr]
    );
    const dateCol = await resolveFuelLotsDateCol();
    const loadsQ = await pool.query(
      `SELECT id, lot_code_created AS lot_code_initial, loaded_liters, ${dateCol} AS load_date, created_at, load_time, seq_index, load_type
         FROM public.fuel_lots
        WHERE unit_id=$1 AND ${dateCol} = $2::date
        ORDER BY COALESCE(load_time, created_at) ASC, id ASC`,
      [truckId, dateStr]
    );
    // Testing activities for the day (only from testing_self_transfers)
    let testingQ = { rows: [] };
    try {
      testingQ = await pool.query(`
        SELECT id, lot_id, from_unit_id, transfer_volume_liters AS testing_volume_liters, performed_at, activity
          FROM public.testing_self_transfers
         WHERE from_unit_id=$1 AND performed_at::date = $2::date
         ORDER BY performed_at ASC, id ASC
      `, [truckId, dateStr]);
    } catch (e) { if (!process.env.SUPPRESS_DB_LOG) console.warn('[ops/day testing warn]', e.message); }

    // Totals for the day
    const totals = {
      sales_liters: salesQ.rows.reduce((a,r)=> a + Number(r.sale_volume_liters||0), 0),
  transfers_out_liters: transfersOutQ.rows.reduce((a,r)=> a + Number(r.transfer_volume||0), 0),
  transfers_in_liters: transfersInQ.rows.reduce((a,r)=> a + Number(r.transfer_volume||0), 0),
      loaded_liters: loadsQ.rows.reduce((a,r)=> a + Number(r.loaded_liters||0), 0),
      testing_liters: testingQ.rows.reduce((a,r)=> a + Number(r.testing_volume_liters||0), 0)
    };

    res.json({
      truck_id: truckId,
      date: dateStr,
      lot: lotInfo,
      remaining_liters: remainingLiters,
      totals,
      sales: salesQ.rows,
      transfers_out: transfersOutQ.rows,
      transfers_in: transfersInQ.rows,
      loads: loadsQ.rows,
      testing: testingQ.rows
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Consolidated per-trip operations filtered by opening/closing window
app.get('/api/fuel-ops/ops/trip', requireAuth, async (req, res) => {
  try {
    const truckId = parseInt(req.query.truck_id, 10);
    const dateStr = isoDateOnly(req.query.date || new Date());
    const tripNo = parseInt(req.query.trip_no, 10);
    if (!Number.isFinite(truckId) || truckId <= 0) return res.status(400).json({ error: 'truck_id required' });
    if (!dateStr) return res.status(400).json({ error: 'invalid date' });
    if (!Number.isFinite(tripNo) || tripNo <= 0) return res.status(400).json({ error: 'trip_no required' });

    const tripQ = await pool.query(
      `SELECT * FROM public.truck_dispenser_trips WHERE truck_id=$1 AND reading_date=$2 AND trip_no=$3`,
      [truckId, dateStr, tripNo]
    );
    if (!tripQ.rows.length) return res.status(404).json({ error: 'trip not found' });
    const trip = tripQ.rows[0];
    const dateCol = await resolveFuelLotsDateCol();
    // Next trip opening to bound upper window if closing_at is null
    const nextQ = await pool.query(
      `SELECT opening_at FROM public.truck_dispenser_trips
        WHERE truck_id=$1 AND reading_date=$2 AND trip_no > $3
        ORDER BY trip_no ASC
        LIMIT 1`,
      [truckId, dateStr, tripNo]
    );
    const defaultStart = `${dateStr} 00:00:00`;
    const defaultEnd = `${dateStr} 23:59:59`;
    // Guard: if opening is not saved yet, this trip has no window; return empty ops for clarity
    if (!trip.opening_at) {
      const loadsQ = await pool.query(
        `SELECT id, lot_code_created AS lot_code_initial, loaded_liters, ${dateCol} AS load_date, created_at, load_time, seq_index, load_type
           FROM public.fuel_lots
          WHERE unit_id=$1 AND ${dateCol} = $2::date
          ORDER BY COALESCE(load_time, created_at) ASC, id ASC`,
        [truckId, dateStr]
      );
      return res.json({
        truck_id: truckId,
        date: dateStr,
        trip_no: tripNo,
        trip,
        totals: { sales_liters: 0, transfers_out_liters: 0, transfers_in_liters: 0, loaded_liters: loadsQ.rows.reduce((a,r)=>a+Number(r.loaded_liters||0),0), testing_liters: 0 },
        sales: [], transfers_out: [], transfers_in: [], loads: loadsQ.rows, testing: []
      });
    }
    // IMPORTANT: Use DB timestamps as-is (no toISOString UTC conversion) to avoid timezone shifts.
    const startSQL = toSqlLocalTs(trip.opening_at) || defaultStart;
    const endSQL = trip.closing_at
      ? (toSqlLocalTs(trip.closing_at) || defaultEnd)
      : (nextQ.rows.length && nextQ.rows[0].opening_at
          ? (toSqlLocalTs(nextQ.rows[0].opening_at) || defaultEnd)
          : defaultEnd);

    // Sales within window
    const salesQ = await pool.query(
      `SELECT id, from_unit_id, from_unit_code, to_vehicle, sale_volume_liters, lot_code_after, driver_name, performed_at, sale_date, activity
         FROM public.fuel_sale_transfers
        WHERE from_unit_id=$1 AND performed_at >= $2::timestamp AND performed_at < $3::timestamp
        ORDER BY COALESCE(performed_at, sale_date) ASC, id ASC`,
      [truckId, startSQL, endSQL]
    );
    const transfersOutQ = await pool.query(
      `SELECT id, from_unit_id, from_unit_code, to_unit_id, to_unit_code, transfer_volume, from_lot_code_change, to_lot_code_change, transfer_to_empty, driver_name, transfer_date, transfer_time, activity
         FROM public.fuel_internal_transfers
        WHERE from_unit_id=$1 AND (transfer_date::timestamp + transfer_time) >= $2::timestamp AND (transfer_date::timestamp + transfer_time) < $3::timestamp
        ORDER BY transfer_date ASC, transfer_time ASC, id ASC`,
      [truckId, startSQL, endSQL]
    );
    const transfersInQ = await pool.query(
      `SELECT id, from_unit_id, from_unit_code, to_unit_id, to_unit_code, transfer_volume, from_lot_code_change, to_lot_code_change, transfer_to_empty, driver_name, transfer_date, transfer_time, activity
         FROM public.fuel_internal_transfers
        WHERE to_unit_id=$1 AND (transfer_date::timestamp + transfer_time) >= $2::timestamp AND (transfer_date::timestamp + transfer_time) < $3::timestamp
        ORDER BY transfer_date ASC, transfer_time ASC, id ASC`,
      [truckId, startSQL, endSQL]
    );
    // Testing within trip window (performed_at between start and end). Only from testing_self_transfers.
    let testingQ = { rows: [] };
    try {
      testingQ = await pool.query(`
        SELECT id, lot_id, from_unit_id, transfer_volume_liters AS testing_volume_liters, performed_at, activity
          FROM public.testing_self_transfers
         WHERE from_unit_id=$1 AND performed_at >= $2::timestamp AND performed_at < $3::timestamp
         ORDER BY performed_at ASC, id ASC
      `, [truckId, startSQL, endSQL]);
    } catch (e) { if (!process.env.SUPPRESS_DB_LOG) console.warn('[ops/trip testing warn]', e.message); }
    // loads are day-level; keep separate for UI reuse
    const loadsQ = await pool.query(
      `SELECT id, lot_code_created AS lot_code_initial, loaded_liters, ${dateCol} AS load_date, created_at, load_time, seq_index, load_type
         FROM public.fuel_lots
        WHERE unit_id=$1 AND ${dateCol} = $2::date
        ORDER BY COALESCE(load_time, created_at) ASC, id ASC`,
      [truckId, dateStr]
    );

    // Compute current in-stock lot and remaining liters (similar to ops/day) so UI can show remaining while in trip-mode
    async function getInboundAddedLiters(lotId) {
      const q = await pool.query(
        `SELECT COALESCE(SUM(transfer_volume),0) AS added
           FROM public.fuel_internal_transfers
          WHERE to_lot_id=$1`, [lotId]
      );
      return Number(q.rows[0]?.added || 0);
    }
    async function getOutboundUsedLiters(lotId) {
      const sales = await pool.query(`SELECT COALESCE(SUM(sale_volume_liters),0) AS s FROM public.fuel_sale_transfers WHERE lot_id=$1`, [lotId]);
      const xfers = await pool.query(`SELECT COALESCE(SUM(transfer_volume),0) AS t FROM public.fuel_internal_transfers WHERE from_lot_id=$1`, [lotId]);
      return Number(sales.rows[0]?.s || 0) + Number(xfers.rows[0]?.t || 0);
    }

    // Determine current in-stock lots for the truck and compute aggregate remaining liters
    const lotsQ = await pool.query(`SELECT * FROM public.fuel_lots WHERE unit_id=$1 AND stock_status='INSTOCK' ORDER BY created_at DESC, id DESC`, [truckId]);
    let lotInfo = null; let remainingLiters = null;
    if (lotsQ.rows.length) {
      let total = 0;
      for (const lot of lotsQ.rows) {
        const inbound = await getInboundAddedLiters(lot.id);
        const outbound = await getOutboundUsedLiters(lot.id);
        const rem = Math.max(0, Number(lot.loaded_liters) + inbound - outbound);
        total += rem;
      }
      remainingLiters = total;
      // Clamp aggregate remaining to unit capacity if available (fetch cap once)
      let cap = 0;
      try {
        const su = await pool.query(`SELECT capacity_liters FROM public.storage_units WHERE id=$1`, [truckId]);
        cap = Number(su.rows[0] && su.rows[0].capacity_liters ? su.rows[0].capacity_liters : 0);
        if (cap > 0 && Number.isFinite(remainingLiters)) {
          remainingLiters = Math.min(remainingLiters, cap);
        }
      } catch (e) { if (!process.env.SUPPRESS_DB_LOG) console.warn('[ops/trip clamp warn]', e.message); }
      // expose the latest lot for display purposes
      const latest = lotsQ.rows[0];
      const latestInbound = await getInboundAddedLiters(latest.id);
      const latestOutbound = await getOutboundUsedLiters(latest.id);
      let latestRemaining = Math.max(0, Number(latest.loaded_liters) + latestInbound - latestOutbound);
      if (cap > 0 && Number.isFinite(latestRemaining)) latestRemaining = Math.min(latestRemaining, cap);
      lotInfo = {
        id: latest.id,
        lot_code_initial: latest.lot_code_created,
        loaded_liters: latest.loaded_liters,
        used_liters: latest.used_liters,
        inbound_adds_liters: latestInbound,
        outbound_used_liters: latestOutbound,
        remaining_liters: latestRemaining
      };
    }

    const totals = {
      sales_liters: salesQ.rows.reduce((a,r)=> a + Number(r.sale_volume_liters||0), 0),
  transfers_out_liters: transfersOutQ.rows.reduce((a,r)=> a + Number(r.transfer_volume||0), 0),
  transfers_in_liters: transfersInQ.rows.reduce((a,r)=> a + Number(r.transfer_volume||0), 0),
      loaded_liters: loadsQ.rows.reduce((a,r)=> a + Number(r.loaded_liters||0), 0),
      testing_liters: testingQ.rows.reduce((a,r)=> a + Number(r.testing_volume_liters||0), 0)
    };

    res.json({
      truck_id: truckId,
      date: dateStr,
      trip_no: tripNo,
      trip,
      totals,
      lot: lotInfo,
      remaining_liters: remainingLiters,
      sales: salesQ.rows,
      transfers_out: transfersOutQ.rows,
      transfers_in: transfersInQ.rows,
      loads: loadsQ.rows,
      testing: testingQ.rows
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Edit/Delete endpoints for transfers (minimal fields; remaining liters computed from sums, so no counter updates here)
app.delete('/api/fuel-ops/transfers/sales/:id', requireAuth, async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (!Number.isFinite(id) || id <= 0) return res.status(400).json({ error: 'invalid id' });
    const del = await pool.query(`DELETE FROM public.fuel_sale_transfers WHERE id=$1 RETURNING *`, [id]);
    if (!del.rows.length) return res.status(404).json({ error: 'not found' });
    res.json({ deleted: true, row: del.rows[0] });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.patch('/api/fuel-ops/transfers/sales/:id', requireAuth, async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (!Number.isFinite(id) || id <= 0) return res.status(400).json({ error: 'invalid id' });
    const { sale_volume_liters, to_vehicle, sale_date, performed_time } = req.body || {};
    // Load existing row to derive base date for performed_at time override
    const existingQ = await pool.query(`SELECT id, performed_at, sale_date FROM public.fuel_sale_transfers WHERE id=$1`, [id]);
    if (!existingQ.rows.length) return res.status(404).json({ error: 'not found' });
    const existing = existingQ.rows[0];
    const parts = [];
    const vals = [];
    let idx = 1;
    if (sale_volume_liters != null) { parts.push(`sale_volume_liters=$${idx++}`); vals.push(parseInt(sale_volume_liters,10)); }
    if (to_vehicle != null) { parts.push(`to_vehicle=$${idx++}`); vals.push(String(to_vehicle)); }
    if (sale_date != null) { parts.push(`sale_date=$${idx++}`); vals.push(isoDateOnly(sale_date)); }
    // Time edit (HH:mm) excluding loads; patch performed_at preserving/deriving date
    if (performed_time != null) {
      const hhmm = String(performed_time).trim();
      if (/^\d{2}:\d{2}$/.test(hhmm)) {
        // Determine date base preference: provided sale_date > existing.sale_date > existing.performed_at date part > CURRENT_DATE
        const baseDate = sale_date ? isoDateOnly(sale_date)
          : (existing.sale_date ? isoDateOnly(existing.sale_date) : (existing.performed_at ? isoDateOnly(existing.performed_at) : isoDateOnly(new Date())));
        if (baseDate) {
          parts.push(`performed_at=$${idx++}`);
          vals.push(`${baseDate} ${hhmm}:00`);
        }
      }
    }
    if (!parts.length) return res.status(400).json({ error: 'no fields to update' });
    parts.push(`updated_at=NOW()`);
    vals.push(id);
    const q = await pool.query(`UPDATE public.fuel_sale_transfers SET ${parts.join(', ')} WHERE id=$${idx} RETURNING *`, vals);
    if (!q.rows.length) return res.status(404).json({ error: 'not found' });
    res.json(q.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/fuel-ops/transfers/internal/:id', requireAuth, async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (!Number.isFinite(id) || id <= 0) return res.status(400).json({ error: 'invalid id' });
    const del = await pool.query(`DELETE FROM public.fuel_internal_transfers WHERE id=$1 RETURNING *`, [id]);
    if (!del.rows.length) return res.status(404).json({ error: 'not found' });
    res.json({ deleted: true, row: del.rows[0] });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.patch('/api/fuel-ops/transfers/internal/:id', requireAuth, async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (!Number.isFinite(id) || id <= 0) return res.status(400).json({ error: 'invalid id' });
  const { transfer_volume_liters, transfer_volume, performed_time, transfer_date } = req.body || {};
    // Load existing row for base date context
  const existingQ = await pool.query(`SELECT id, transfer_date, transfer_time FROM public.fuel_internal_transfers WHERE id=$1`, [id]);
    if (!existingQ.rows.length) return res.status(404).json({ error: 'not found' });
    const existing = existingQ.rows[0];
    const parts = [];
    const vals = [];
    let idx = 1;
  if (transfer_volume != null) { parts.push(`transfer_volume=$${idx++}`); vals.push(parseInt(transfer_volume,10)); }
  else if (transfer_volume_liters != null) { parts.push(`transfer_volume=$${idx++}`); vals.push(parseInt(transfer_volume_liters,10)); }
    if (transfer_date != null) { parts.push(`transfer_date=$${idx++}`); vals.push(isoDateOnly(transfer_date)); }
    if (performed_time != null) {
      const hhmm = String(performed_time).trim();
      if (/^\d{2}:\d{2}$/.test(hhmm)) {
        parts.push(`transfer_time=$${idx++}`);
        vals.push(`${hhmm}:00`);
      }
    }
    if (!parts.length) return res.status(400).json({ error: 'no fields to update' });
    parts.push(`updated_at=NOW()`);
    vals.push(id);
    const q = await pool.query(`UPDATE public.fuel_internal_transfers SET ${parts.join(', ')} WHERE id=$${idx} RETURNING *`, vals);
    if (!q.rows.length) return res.status(404).json({ error: 'not found' });
    res.json(q.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Full update of an internal transfer, including activity, date/time, from/to units, volume, and driver
// Recomputes lot pointers and lot statuses to remain consistent with append-only sums logic
app.put('/api/fuel-ops/transfers/internal/:id/full', requireAuth, async (req, res) => {
  const client = await pool.connect();
  try {
    const id = parseInt(req.params.id, 10);
    if (!Number.isFinite(id) || id <= 0) return res.status(400).json({ error: 'invalid id' });
    const { activity, from_unit_id, to_unit_id, volume_liters, driver_id, transfer_date, performed_time } = req.body || {};
    const act = String(activity || '').toUpperCase();
    if (!new Set(['TANKER_TO_TANKER','TANKER_TO_DATUM']).has(act)) return res.status(400).json({ error: 'invalid activity' });
    const fromId = parseInt(from_unit_id, 10);
    const toId = parseInt(to_unit_id, 10);
    const vol = parseInt(volume_liters, 10);
    if (!Number.isFinite(fromId) || fromId <= 0) return res.status(400).json({ error: 'from_unit_id required' });
    if (!Number.isFinite(toId) || toId <= 0) return res.status(400).json({ error: 'to_unit_id required' });
    if (!Number.isFinite(vol) || vol <= 0) return res.status(400).json({ error: 'volume_liters must be > 0' });

    await client.query('BEGIN');
    const existingQ = await client.query(`SELECT * FROM public.fuel_internal_transfers WHERE id=$1 FOR UPDATE`, [id]);
    if (!existingQ.rows.length) { await client.query('ROLLBACK'); return res.status(404).json({ error: 'not found' }); }
    const existing = existingQ.rows[0];

    // Resolve driver (optional)
    let drow = null;
    if (driver_id != null) {
      const dr = await client.query(`SELECT id, name, driver_id FROM public.drivers WHERE id=$1`, [parseInt(driver_id,10)]);
      drow = dr.rows[0] || null;
    }

    // Determine date-only and timestamp
    const dateOnly = transfer_date ? isoDateOnly(transfer_date) : (existing.transfer_date ? isoDateOnly(existing.transfer_date) : isoDateOnly(new Date()));
    const hhmm = (performed_time || '').trim();
    const tsSql = (/^\d{2}:\d{2}$/.test(hhmm) && dateOnly) ? `${dateOnly} ${hhmm}:00` : (dateOnly ? `${dateOnly} 00:00:00` : null);

    // Helper: inbound added (exclude seeding) for a lot
    async function getInboundAddedLiters(c, lotId) {
      const q = await c.query(
        `SELECT COALESCE(SUM(fit.transfer_volume),0) AS added
           FROM public.fuel_internal_transfers fit
           JOIN public.fuel_lots fl ON fl.id = fit.to_lot_id
          WHERE fit.to_lot_id=$1
            AND NOT (
              fit.transfer_to_empty = TRUE
                   OR (fit.to_lot_code_change = fl.lot_code_created AND fit.transfer_volume = fl.loaded_liters)
            )`,
        [lotId]
      );
      return Number(q.rows[0]?.added || 0);
    }
    // Helper: outbound used (sales + internal transfers) for a lot
    async function getOutboundUsedLiters(c, lotId) {
      const sales = await c.query(`SELECT COALESCE(SUM(sale_volume_liters),0) AS s FROM public.fuel_sale_transfers WHERE lot_id=$1`, [lotId]);
  const xfers = await c.query(`SELECT COALESCE(SUM(transfer_volume),0) AS t FROM public.fuel_internal_transfers WHERE from_lot_id=$1`, [lotId]);
      return Number(sales.rows[0]?.s || 0) + Number(xfers.rows[0]?.t || 0);
    }

    // Resolve unit codes and lots
    const fromUnit = await client.query(`SELECT id, unit_code, unit_type, capacity_liters FROM public.storage_units WHERE id=$1`, [fromId]);
    const toUnit = await client.query(`SELECT id, unit_code, unit_type, capacity_liters FROM public.storage_units WHERE id=$1`, [toId]);
    if (!fromUnit.rows.length) { await client.query('ROLLBACK'); return res.status(400).json({ error: 'invalid from_unit_id' }); }
    if (!toUnit.rows.length) { await client.query('ROLLBACK'); return res.status(400).json({ error: 'invalid to_unit_id' }); }
    const fromCode = fromUnit.rows[0].unit_code;
    const toCode = toUnit.rows[0].unit_code;

    // Find in-stock lots for from/to; if to has none, create EMPTY_TRANSFER lot seeded with this transfer volume
    const lotFromQ = await client.query(`SELECT * FROM public.fuel_lots WHERE unit_id=$1 AND stock_status='INSTOCK' ORDER BY created_at DESC, id DESC LIMIT 1`, [fromId]);
    if (!lotFromQ.rows.length) { await client.query('ROLLBACK'); return res.status(400).json({ error: 'No in-stock lot for source unit' }); }
    const lotFrom = lotFromQ.rows[0];

  let lotToQ = await client.query(`SELECT * FROM public.fuel_lots WHERE unit_id=$1 AND stock_status='INSTOCK' ORDER BY created_at DESC, id DESC LIMIT 1`, [toId]);
    let createdNewDestLot = false;
    if (!lotToQ.rows.length) {
      const tRow = toUnit.rows[0];
      if (tRow && (tRow.unit_type === 'DATUM' || tRow.unit_type === 'TRUCK')) {
        // capacity guard
        const cap = Number(tRow.capacity_liters || 0);
        if (cap > 0 && vol > cap) { await client.query('ROLLBACK'); return res.status(400).json({ error: `destination capacity exceeded: would be ${vol}/${cap}` }); }
        const dateCol = await resolveFuelLotsDateCol();
        const created = await client.query(`
          WITH seq AS (
            SELECT COALESCE(MAX(seq_index),0)+1 AS next
              FROM public.fuel_lots
             WHERE unit_id=$1 AND ${dateCol} = CURRENT_DATE
          )
          INSERT INTO public.fuel_lots (
            unit_id, tanker_code, tanker_capacity, ${dateCol}, seq_index, seq_letters,
            loaded_liters, lot_code_created, stock_status, used_liters, updated_at, load_type
          )
          SELECT $1, $2, $3, CURRENT_DATE, s.next, public.seq_index_to_letters(s.next),
                 $4, public.gen_lot_code($2, CURRENT_DATE, s.next, $4), 'INSTOCK', 0, NOW(), 'EMPTY_TRANSFER'
            FROM seq s
            RETURNING *
        `, [toId, toCode, toUnit.rows[0].capacity_liters, vol]);
        lotToQ = created; createdNewDestLot = true;
      }
    }
    if (!lotToQ.rows.length) { await client.query('ROLLBACK'); return res.status(400).json({ error: 'No in-stock lot for destination unit' }); }
    const lotTo = lotToQ.rows[0];

    // Capacity guard for destination with current net
    const destCap = Number(toUnit.rows[0].capacity_liters || 0);
    if (destCap > 0) {
      const toAddedBefore = createdNewDestLot ? 0 : await getInboundAddedLiters(client, lotTo.id);
      const toUsedBefore = createdNewDestLot ? 0 : await getOutboundUsedLiters(client, lotTo.id);
      const toCurrentNet = (createdNewDestLot ? 0 : (Number(lotTo.loaded_liters) + toAddedBefore - toUsedBefore));
      const toNetAfter = toCurrentNet + vol;
      if (toNetAfter > destCap) { await client.query('ROLLBACK'); return res.status(400).json({ error: `destination capacity exceeded: would be ${toNetAfter}/${destCap}` }); }
    }

    // Update transfer row core fields first (lot pointers, units, volume, activity, driver, timestamps, date)
   const upd1 = await client.query(`
    UPDATE public.fuel_internal_transfers
      SET from_lot_id=$2, to_lot_id=$3,
         from_unit_id=$4, to_unit_id=$5,
         from_unit_code=$6, to_unit_code=$7,
         transfer_volume=$8,
         activity=$9,
         driver_name=$10,
         transfer_time=COALESCE($11::time, transfer_time),
         transfer_date=COALESCE($12::date, transfer_date),
         updated_at=NOW()
     WHERE id=$1
     RETURNING *
   `, [id, lotFrom.id, lotTo.id, fromId, toId, fromCode, toCode, vol, act, drow ? drow.name : null, (tsSql ? hhmm : null), dateOnly]);
    if (!upd1.rows.length) { await client.query('ROLLBACK'); return res.status(404).json({ error: 'not found after update' }); }

    // Recompute used/inbound to build after codes and adjust lot stock and used
    const fromAddedCum = await getInboundAddedLiters(client, lotFrom.id);
    const fromUsedNow = await getOutboundUsedLiters(client, lotFrom.id);
    const fromSuffix = `-${fromUsedNow}` + (fromAddedCum > 0 ? `+(${fromAddedCum})` : '');
  const fromLotCodeAfter = `${lotFrom.lot_code_created}${fromSuffix}`;

    const toAddedAfter = createdNewDestLot ? 0 : await getInboundAddedLiters(client, lotTo.id);
    const toUsedOut = createdNewDestLot ? 0 : await getOutboundUsedLiters(client, lotTo.id);
    const toSuffix = createdNewDestLot ? '' : (`-${Number(lotTo.used_liters || 0)}` + (toAddedAfter > 0 ? `+(${toAddedAfter})` : ''));
  const toLotCodeAfter = `${lotTo.lot_code_created}${toSuffix}`;

  await client.query(`UPDATE public.fuel_internal_transfers SET from_lot_code_change=$2, to_lot_code_change=$3 WHERE id=$1`, [id, fromLotCodeAfter, toLotCodeAfter]);

    // Ensure destination lot reflects the actual purchase time when it represents an EMPTY_TRANSFER seed.
    // Case 1: Lot got created in this update (createdNewDestLot) and we have a performed_time -> set load_time.
    // Case 2: Lot was created earlier without time and user edited performed_time later -> also set load_time
    //         as long as the destination lot is an EMPTY_TRANSFER lot.
    try {
      if (tsSql) {
        const shouldStampLoadTime = createdNewDestLot || (lotTo && String(lotTo.load_type).toUpperCase() === 'EMPTY_TRANSFER');
        if (shouldStampLoadTime) {
          await client.query(`UPDATE public.fuel_lots SET load_time=$1::timestamp WHERE id=$2`, [tsSql, lotTo.id]);
        }
      }
    } catch (e) { if (!process.env.SUPPRESS_DB_LOG) console.warn('[warn] full update set load_time failed', e.message); }

    // Update from lot used_liters and stock status
    const fromNetRemaining = (Number(lotFrom.loaded_liters) + fromAddedCum) - fromUsedNow;
    const fromStock = fromNetRemaining <= 0 ? 'SOLD' : 'INSTOCK';
    await client.query(`UPDATE public.fuel_lots SET used_liters=$2, stock_status=$3, updated_at=NOW() WHERE id=$1`, [lotFrom.id, fromUsedNow, fromStock]);

    // Update to lot stock status based on net remaining
    const toNetRemaining = (Number(lotTo.loaded_liters) + (createdNewDestLot ? 0 : toAddedAfter)) - Number(lotTo.used_liters || 0);
    const toStock = toNetRemaining <= 0 ? 'SOLD' : 'INSTOCK';
    await client.query(`UPDATE public.fuel_lots SET stock_status=$2, updated_at=NOW() WHERE id=$1`, [lotTo.id, toStock]);

    await client.query('COMMIT');
    const finalQ = await pool.query(`SELECT * FROM public.fuel_internal_transfers WHERE id=$1`, [id]);
    res.json(finalQ.rows[0]);
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch {}
    res.status(500).json({ error: e.message });
  } finally {
    client.release();
  }
});

// dispenser_readings table removed — use meter snapshots instead
app.post('/api/fuel-ops/readings/dispenser', requireAuth, async (req, res) => {
  res.status(410).json({ error: 'dispenser_readings removed; use /api/fuel-ops/meter-snapshots to record dispenser meter readings' });
});
app.get('/api/fuel-ops/readings/dispenser', requireAuth, async (req, res) => {
  res.status(410).json({ error: 'dispenser_readings removed; use /api/fuel-ops/meter-snapshots to list dispenser meter snapshots' });
});

// truck_odometer_readings table removed — use daily odometer rollups or other odometer endpoints
app.post('/api/fuel-ops/readings/odometer', requireAuth, async (req, res) => {
  res.status(410).json({ error: 'truck_odometer_readings removed; use /api/fuel-ops/day/odometer for daily odometer records' });
});
app.get('/api/fuel-ops/readings/odometer', requireAuth, async (req, res) => {
  res.status(410).json({ error: 'truck_odometer_readings removed; use /api/fuel-ops/day/odometer and /api/fuel-ops/day/odometer/list' });
});

// --- Reminders summaries (Overview and Assigned To) ---
// Helper: format start-of-day for N days from now in server local time
function localStartOfDayPlus(days = 0) {
  const d = new Date();
  d.setHours(0, 0, 0, 0);
  d.setDate(d.getDate() + (Number.isFinite(days) ? days : 0));
  const pad = n => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} 00:00:00`;
}

// Helpers for this week/month boundaries (local-time)
function localStartOfWeek() {
  const d = new Date();
  d.setHours(0,0,0,0);
  // Monday as start of week
  const day = d.getDay(); // 0=Sun ... 6=Sat
  const offset = (day === 0 ? -6 : 1 - day);
  d.setDate(d.getDate() + offset);
  const pad = n => String(n).padStart(2,'0');
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} 00:00:00`;
}
function localStartOfNextWeek() {
  const d = new Date();
  d.setHours(0,0,0,0);
  const day = d.getDay();
  const offsetToMonday = (day === 0 ? -6 : 1 - day);
  d.setDate(d.getDate() + offsetToMonday + 7);
  const pad = n => String(n).padStart(2,'0');
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} 00:00:00`;
}
function localStartOfMonth() {
  const d = new Date();
  d.setHours(0,0,0,0);
  d.setDate(1);
  const pad = n => String(n).padStart(2,'0');
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-01 00:00:00`;
}
function localStartOfNextMonth() {
  const d = new Date();
  d.setHours(0,0,0,0);
  d.setDate(1);
  d.setMonth(d.getMonth()+1);
  const pad = n => String(n).padStart(2,'0');
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-01 00:00:00`;
}
// Helpers aligning to client 'scopes' behavior
function startOfWeekFor(date) {
  const d = new Date(date.getTime());
  d.setHours(0,0,0,0);
  const day = d.getDay(); // 0=Sun..6=Sat
  const offset = (day === 0 ? -6 : 1 - day); // Monday start
  d.setDate(d.getDate() + offset);
  const pad = n => String(n).padStart(2,'0');
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} 00:00:00`;
}
function addDaysSQL(sqlDateString, days) {
  // postgres can add interval '1 day'
  // We'll use SQL arithmetic via parameters instead; here we leave for clarity if needed.
  return null;
}

// GET /api/reminders/summary/overview
// Optional query: userId (OWNER/ADMIN only to view other users)
// Buckets: delayed (PENDING before today), today, tomorrow
app.get('/api/reminders/summary/overview', requireAuth, async (req, res) => {
  try {
    // Determine target user (self by default)
    let targetUserId = req.user && req.user.sub;
    const requestedUserId = req.query.userId ? String(req.query.userId) : null;
    if (requestedUserId && req.user && (req.user.role === 'OWNER' || req.user.role === 'ADMIN')) {
      // Validate selectable (exclude ADMIN)
      const valid = await validateSelectableUserId(pool, requestedUserId);
      if (!valid) return res.status(400).json({ error: 'Invalid userId' });
      targetUserId = valid;
    }
    if (!targetUserId) return res.status(400).json({ error: 'Unable to resolve user' });

    // Date buckets (local)
    const startToday = localStartOfDayPlus(0);
    const startTomorrow = localStartOfDayPlus(1);
    const startDayAfter = localStartOfDayPlus(2);

    // Scope: reminders created by or assigned to target user, or linked meetings created/assigned to target user
    const params = [targetUserId, targetUserId, targetUserId, startToday, startTomorrow, startDayAfter];
    const idents = [req.user.email, req.user.username, req.user.full_name].filter(v => v && String(v).trim().length);
    let legacyCreated = '';
    let legacyAssigned = '';
    let legacyMeetings = '';
    if (idents.length) {
      const base = params.length;
      params.push(...idents);
      const ph = idents.map((_, i) => `$${base + i + 1}`).join(',');
      legacyCreated = ` OR r.created_by IN (${ph})`;
      const base2 = params.length;
      params.push(...idents);
      const ph2 = idents.map((_, i) => `$${base2 + i + 1}`).join(',');
      legacyAssigned = ` OR r.assigned_to IN (${ph2})`;
      // meetings legacy
      const base3 = params.length;
      params.push(...idents);
      const ph3 = idents.map((_, i) => `$${base3 + i + 1}`).join(',');
      const base4 = params.length;
      params.push(...idents);
      const ph4 = idents.map((_, i) => `$${base4 + i + 1}`).join(',');
      legacyMeetings = ` OR r.meeting_id IN (SELECT id FROM meetings WHERE assigned_to IN (${ph3}) OR created_by IN (${ph4}))`;
    }

    const scope = `(
      r.created_by_user_id = $1
      OR r.assigned_to_user_id = $2
      OR r.meeting_id IN (
           SELECT id FROM meetings WHERE assigned_to_user_id = $3 OR created_by_user_id = $3
        )
      ${legacyCreated}
      ${legacyAssigned}
      ${legacyMeetings}
    )`;

    const sql = `
      WITH base AS (
        SELECT r.type, r.status,
               CASE
                 WHEN r.status = 'PENDING' AND r.due_ts < $4 THEN 'DELAYED'
                 WHEN r.due_ts >= $4 AND r.due_ts < $5 THEN 'TODAY'
                 WHEN r.due_ts >= $5 AND r.due_ts < $6 THEN 'TOMORROW'
                 ELSE NULL
               END AS bucket
          FROM reminders r
         WHERE ${scope}
           AND r.type IN ('CALL','EMAIL')
           AND (
                (r.status = 'PENDING' AND r.due_ts < $4)
                OR (r.due_ts >= $4 AND r.due_ts < $6)
           )
      )
      SELECT bucket, type, status, COUNT(*)::int AS count
        FROM base
       WHERE bucket IS NOT NULL
       GROUP BY bucket, type, status
    `;
    const r = await pool.query(sql, params);
    const buckets = { delayed: {}, today: {}, tomorrow: {} };
    for (const row of r.rows) {
      const b = String(row.bucket || '').toLowerCase();
      if (!buckets[b]) buckets[b] = {};
      if (!buckets[b][row.type]) buckets[b][row.type] = {};
      buckets[b][row.type][row.status] = Number(row.count) || 0;
    }
    return res.json({ buckets, generatedAt: new Date().toISOString() });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/reminders/summary/assigned
// Query: assignedToUserId (optional), createdBy=self (optional flag; if present and not 'self', ignored)
app.get('/api/reminders/summary/assigned', requireAuth, async (req, res) => {
  try {
    // Date buckets (local)
    const startToday = localStartOfDayPlus(0);
    const startTomorrow = localStartOfDayPlus(1);
    const startDayAfter = localStartOfDayPlus(2);

  const params = [startToday, startTomorrow, startDayAfter];

    // Scope: by default, only reminders created by self (aligns with Assigned To page semantics)
    // Determine whether to enforce createdBy=self:
    // - Always enforce for EMPLOYEE
    // - For OWNER/ADMIN: enforce only when createdBy=self is explicitly requested
    const forceCreatedBySelf = (req.user && req.user.role === 'EMPLOYEE') || (String(req.query.createdBy || '').toLowerCase() === 'self');
    let creatorScope = '';
    if (forceCreatedBySelf) {
      if (req.user && req.user.sub) {
        params.push(req.user.sub);
        const base = params.length;
        creatorScope = `(r.created_by_user_id = $${base}`;
        const idents = [req.user.email, req.user.username, req.user.full_name].filter(v => v && String(v).trim().length);
        if (idents.length) {
          const base2 = params.length;
          params.push(...idents);
          const ph = idents.map((_, i) => `$${base2 + i + 1}`).join(',');
          creatorScope += ` OR r.created_by IN (${ph})`;
        }
        creatorScope += ')';
      } else {
        return res.status(400).json({ error: 'Unauthorized' });
      }
    }

    // Optional filter: assignedToUserId
    const assignedToUserId = req.query.assignedToUserId ? String(req.query.assignedToUserId) : null;
    let assigneeFilter = '';
    if (assignedToUserId) {
      if (req.user && (req.user.role === 'OWNER' || req.user.role === 'ADMIN')) {
        const valid = await validateSelectableUserId(pool, assignedToUserId);
        if (!valid) return res.status(400).json({ error: 'Invalid assignedToUserId' });
        params.push(valid);
        const base = params.length;
        assigneeFilter = ` AND (r.assigned_to_user_id = $${base}`;
        const ur = await pool.query('SELECT email, username, full_name FROM public.users WHERE id=$1', [valid]);
        const parts = ur.rows.length ? [ur.rows[0].email, ur.rows[0].username, ur.rows[0].full_name].filter(v => v && String(v).trim().length) : [];
        if (parts.length) {
          const base2 = params.length;
          params.push(...parts);
          const ph = parts.map((_, i) => `$${base2 + i + 1}`).join(',');
          assigneeFilter += ` OR r.assigned_to IN (${ph})`;
        }
        assigneeFilter += ')';
      } else if (req.user && req.user.role === 'EMPLOYEE') {
        // EMPLOYEE can only filter to self or if createdBy=self is enforced (already enforced above)
        const allowedUserId = (String(assignedToUserId) === String(req.user.sub)) ? req.user.sub : await validateSelectableUserId(pool, assignedToUserId);
        if (!allowedUserId) return res.status(403).json({ error: 'Forbidden assignedToUserId' });
        params.push(allowedUserId);
        const base = params.length;
        assigneeFilter = ` AND (r.assigned_to_user_id = $${base}`;
        const ur = await pool.query('SELECT email, username, full_name FROM public.users WHERE id=$1', [allowedUserId]);
        const parts = ur.rows.length ? [ur.rows[0].email, ur.rows[0].username, ur.rows[0].full_name].filter(v => v && String(v).trim().length) : [];
        if (parts.length) {
          const base2 = params.length;
          params.push(...parts);
          const ph = parts.map((_, i) => `$${base2 + i + 1}`).join(',');
          assigneeFilter += ` OR r.assigned_to IN (${ph})`;
        }
        assigneeFilter += ')';
      }
    }

    const sql = `
      WITH scoped AS (
        SELECT r.type, r.status,
               CASE
                 WHEN r.status = 'PENDING' AND r.due_ts < $1 THEN 'DELAYED'
                 WHEN r.due_ts >= $1 AND r.due_ts < $2 THEN 'TODAY'
                 WHEN r.due_ts >= $2 AND r.due_ts < $3 THEN 'TOMORROW'
                 ELSE NULL
               END AS bucket
          FROM reminders r
      WHERE ${(creatorScope ? creatorScope + '\n           ' : '')}${assigneeFilter}
           AND r.type IN ('CALL','EMAIL')
           AND (
                (r.status = 'PENDING' AND r.due_ts < $1)
                OR (r.due_ts >= $1 AND r.due_ts < $3)
           )
      )
      SELECT bucket, type, status, COUNT(*)::int AS count
        FROM scoped
       WHERE bucket IS NOT NULL
       GROUP BY bucket, type, status
    `;
    const r = await pool.query(sql, params);
    const buckets = { delayed: {}, today: {}, tomorrow: {} };
    for (const row of r.rows) {
      const b = String(row.bucket || '').toLowerCase();
      if (!buckets[b]) buckets[b] = {};
      if (!buckets[b][row.type]) buckets[b][row.type] = {};
      buckets[b][row.type][row.status] = Number(row.count) || 0;
    }
    return res.json({ buckets, generatedAt: new Date().toISOString() });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// Extended overview summary for My Overview tab
// Returns counts for: total, today, tomorrow, this week, this month
// Each group includes: overdue (delayed PENDING), pending (non-delayed), done, sent, failed
app.get('/api/reminders/summary/overview-extended', requireAuth, async (req, res) => {
  try {
    // Determine target user (self by default; OWNER/ADMIN may pass userId)
    let targetUserId = req.user && req.user.sub;
    const requestedUserId = req.query.userId ? String(req.query.userId) : null;
    if (requestedUserId && req.user && (req.user.role === 'OWNER' || req.user.role === 'ADMIN')) {
      const valid = await validateSelectableUserId(pool, requestedUserId);
      if (!valid) return res.status(400).json({ error: 'Invalid userId' });
      targetUserId = valid;
    }
    if (!targetUserId) return res.status(400).json({ error: 'Unable to resolve user' });

  // Dates
  const nowTs = formatLocalSQL(new Date());
  const startToday = localStartOfDayPlus(0);
    const startTomorrow = localStartOfDayPlus(1);
    const startDayAfter = localStartOfDayPlus(2);
  // Week/Month windows must match client scopes:
  // - week: week that contains tomorrow (Mon..Sun)
  // - month: the 3 weeks immediately after that week (rolling window)
  const tomorrow = new Date();
  tomorrow.setDate(tomorrow.getDate() + 1);
  tomorrow.setHours(0,0,0,0);
  const startWeek = startOfWeekFor(tomorrow); // Monday 00:00:00 of week containing tomorrow (as local SQL string)
  // Compute windows as concrete timestamps to avoid inline SQL arithmetic/quoting issues
  const startWeekDate = toLocalDate(startWeek);
  const plusDays = (d, n) => { const x = new Date(d.getTime()); x.setDate(x.getDate() + n); return x; };
  const startNextWeekTs = formatLocalSQL(plusDays(startWeekDate, 7)); // exclusive upper bound for week
  const startMonthTs = startNextWeekTs;                               // month starts right after that week
  const startNextMonthTs = formatLocalSQL(plusDays(startWeekDate, 28)); // week start + 28 days

    // Scope conditions as in overview endpoint
  // Note: startNextWeek/startMonth/startNextMonth are SQL expressions; we'll inline them into the query
  const params = [targetUserId, targetUserId, targetUserId, nowTs, startToday, startTomorrow, startDayAfter, startWeek, startNextWeekTs, startMonthTs, startNextMonthTs];
    const idents = [req.user.email, req.user.username, req.user.full_name].filter(v => v && String(v).trim().length);
    let legacyCreated = '';
    let legacyAssigned = '';
    let legacyMeetings = '';
    if (idents.length) {
      const base = params.length;
      params.push(...idents);
      const ph = idents.map((_, i) => `$${base + i + 1}`).join(',');
      legacyCreated = ` OR r.created_by IN (${ph})`;
      const base2 = params.length;
      params.push(...idents);
      const ph2 = idents.map((_, i) => `$${base2 + i + 1}`).join(',');
      legacyAssigned = ` OR r.assigned_to IN (${ph2})`;
      const base3 = params.length;
      params.push(...idents);
      const ph3 = idents.map((_, i) => `$${base3 + i + 1}`).join(',');
      const base4 = params.length;
      params.push(...idents);
      const ph4 = idents.map((_, i) => `$${base4 + i + 1}`).join(',');
      legacyMeetings = ` OR r.meeting_id IN (SELECT id FROM meetings WHERE assigned_to IN (${ph3}) OR created_by IN (${ph4}))`;
    }
    const scope = `(
      r.created_by_user_id = $1
      OR r.assigned_to_user_id = $2
      OR r.meeting_id IN (SELECT id FROM meetings WHERE assigned_to_user_id = $3 OR created_by_user_id = $3)
      ${legacyCreated}
      ${legacyAssigned}
      ${legacyMeetings}
    )`;
    const sql = `
      WITH scoped AS (
        SELECT r.status, r.due_ts
          FROM reminders r
         WHERE ${scope}
           AND r.type IN ('CALL','EMAIL')
      )
      SELECT
        COUNT(*) FILTER (WHERE status='PENDING' AND due_ts < LOCALTIMESTAMP)::int AS overdue_global,
        COUNT(*) FILTER (WHERE status='PENDING' AND due_ts >= LOCALTIMESTAMP)::int AS pending_total,
        COUNT(*) FILTER (WHERE status='DONE')::int AS done_total,
        COUNT(*) FILTER (WHERE status='SENT')::int AS sent_total,
        COUNT(*) FILTER (WHERE status='FAILED')::int AS failed_total,
        COUNT(*) FILTER (WHERE status='PENDING' AND due_ts >= $5 AND due_ts < $6 AND due_ts >= LOCALTIMESTAMP)::int AS pending_today,
        COUNT(*) FILTER (WHERE status='DONE' AND due_ts >= $5 AND due_ts < $6)::int AS done_today,
        COUNT(*) FILTER (WHERE status='SENT' AND due_ts >= $5 AND due_ts < $6)::int AS sent_today,
        COUNT(*) FILTER (WHERE status='FAILED' AND due_ts >= $5 AND due_ts < $6)::int AS failed_today,
        COUNT(*) FILTER (WHERE status='PENDING' AND due_ts >= $6 AND due_ts < $7)::int AS pending_tomorrow,
        COUNT(*) FILTER (WHERE status='DONE' AND due_ts >= $6 AND due_ts < $7)::int AS done_tomorrow,
        COUNT(*) FILTER (WHERE status='SENT' AND due_ts >= $6 AND due_ts < $7)::int AS sent_tomorrow,
        COUNT(*) FILTER (WHERE status='FAILED' AND due_ts >= $6 AND due_ts < $7)::int AS failed_tomorrow,
        COUNT(*) FILTER (WHERE status='PENDING' AND due_ts >= $8 AND due_ts < $9)::int AS pending_week,
        COUNT(*) FILTER (WHERE status='DONE' AND due_ts >= $8 AND due_ts < $9)::int AS done_week,
        COUNT(*) FILTER (WHERE status='SENT' AND due_ts >= $8 AND due_ts < $9)::int AS sent_week,
        COUNT(*) FILTER (WHERE status='FAILED' AND due_ts >= $8 AND due_ts < $9)::int AS failed_week,
        COUNT(*) FILTER (WHERE status='PENDING' AND due_ts >= $10 AND due_ts < $11)::int AS pending_month,
        COUNT(*) FILTER (WHERE status='DONE' AND due_ts >= $10 AND due_ts < $11)::int AS done_month,
        COUNT(*) FILTER (WHERE status='SENT' AND due_ts >= $10 AND due_ts < $11)::int AS sent_month,
        COUNT(*) FILTER (WHERE status='FAILED' AND due_ts >= $10 AND due_ts < $11)::int AS failed_month
      FROM scoped
    `;
    const rows = await pool.query(sql, params);
    const row = rows.rows[0] || {};
    const overdueGlobal = Number(row.overdue_global || 0);
    const result = {
      total: {
        overdue: overdueGlobal,
        pending: Number(row.pending_total || 0),
        done: Number(row.done_total || 0),
        sent: Number(row.sent_total || 0),
        failed: Number(row.failed_total || 0)
      },
      today: {
        overdue: overdueGlobal,
        pending: Number(row.pending_today || 0),
        done: Number(row.done_today || 0),
        sent: Number(row.sent_today || 0),
        failed: Number(row.failed_today || 0)
      },
      tomorrow: {
        overdue: 0,
        pending: Number(row.pending_tomorrow || 0),
        done: Number(row.done_tomorrow || 0),
        sent: Number(row.sent_tomorrow || 0),
        failed: Number(row.failed_tomorrow || 0)
      },
      week: {
        overdue: 0,
        pending: Number(row.pending_week || 0),
        done: Number(row.done_week || 0),
        sent: Number(row.sent_week || 0),
        failed: Number(row.failed_week || 0)
      },
      month: {
        overdue: 0,
        pending: Number(row.pending_month || 0),
        done: Number(row.done_month || 0),
        sent: Number(row.sent_month || 0),
        failed: Number(row.failed_month || 0)
      },
      generatedAt: new Date().toISOString()
    };
    res.json(result);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// Extended summary for Employee Overview (OWNER/ADMIN aggregate or per-user)
// Query params: optional userId (OWNER/ADMIN may select any selectable user; EMPLOYEE only self)
// Returns counts for: total, today, tomorrow, this week, this month
// Each group includes: overdue (delayed PENDING), pending (non-delayed), done, sent, failed
app.get('/api/reminders/summary/employee-extended', requireAuth, async (req, res) => {
  try {
    const role = (req.user && req.user.role) || 'EMPLOYEE';
    const requestedUserId = req.query.userId ? String(req.query.userId) : null;

    // Resolve per-user scope (if any), with role-aware guard
    let scopeSql = '';
    const params = [];
    if (requestedUserId) {
      if (role === 'OWNER' || role === 'ADMIN') {
        const valid = await validateSelectableUserId(pool, requestedUserId);
        if (!valid) return res.status(400).json({ error: 'Invalid userId' });
        // param order: [created_by_user_id, assigned_to_user_id, meeting.assigned_to_user_id, meeting.created_by_user_id, ... legacy idents]
        params.push(valid, valid, valid, valid);
        // Also support legacy string columns
        const ur = await pool.query('SELECT email, username, full_name FROM public.users WHERE id=$1', [valid]);
        const idents = ur.rows.length ? [ur.rows[0].email, ur.rows[0].username, ur.rows[0].full_name].filter(v => v && String(v).trim().length) : [];
        let legacyCreated = '';
        let legacyAssigned = '';
        let legacyMeetings = '';
        if (idents.length) {
          const base = params.length;
          params.push(...idents);
          const ph = idents.map((_, i) => `$${base + i + 1}`).join(',');
          legacyCreated = ` OR r.created_by IN (${ph})`;
          const base2 = params.length;
          params.push(...idents);
          const ph2 = idents.map((_, i) => `$${base2 + i + 1}`).join(',');
          legacyAssigned = ` OR r.assigned_to IN (${ph2})`;
          const base3 = params.length;
          params.push(...idents);
          const ph3 = idents.map((_, i) => `$${base3 + i + 1}`).join(',');
          const base4 = params.length;
          params.push(...idents);
          const ph4 = idents.map((_, i) => `$${base4 + i + 1}`).join(',');
          legacyMeetings = ` OR r.meeting_id IN (SELECT id FROM meetings WHERE assigned_to IN (${ph3}) OR created_by IN (${ph4}))`;
        }
        scopeSql = `(
          r.created_by_user_id = $1
          OR r.assigned_to_user_id = $2
          OR r.meeting_id IN (SELECT id FROM meetings WHERE assigned_to_user_id = $3 OR created_by_user_id = $4)
          ${legacyCreated}
          ${legacyAssigned}
          ${legacyMeetings}
        )`;
      } else if (role === 'EMPLOYEE') {
        // Employees may only query themselves
        const selfId = req.user && req.user.sub;
        if (!selfId || String(requestedUserId) !== String(selfId)) return res.status(403).json({ error: 'Forbidden' });
        params.push(selfId, selfId, selfId, selfId);
        const ids = [req.user.email, req.user.username, req.user.full_name].filter(v => v && String(v).trim().length);
        let legacyCreated = '';
        let legacyAssigned = '';
        let legacyMeetings = '';
        if (ids.length) {
          const base = params.length;
          params.push(...ids);
          const ph = ids.map((_, i) => `$${base + i + 1}`).join(',');
          legacyCreated = ` OR r.created_by IN (${ph})`;
          const base2 = params.length;
          params.push(...ids);
          const ph2 = ids.map((_, i) => `$${base2 + i + 1}`).join(',');
          legacyAssigned = ` OR r.assigned_to IN (${ph2})`;
          const base3 = params.length;
          params.push(...ids);
          const ph3 = ids.map((_, i) => `$${base3 + i + 1}`).join(',');
          const base4 = params.length;
          params.push(...ids);
          const ph4 = ids.map((_, i) => `$${base4 + i + 1}`).join(',');
          legacyMeetings = ` OR r.meeting_id IN (SELECT id FROM meetings WHERE assigned_to IN (${ph3}) OR created_by IN (${ph4}))`;
        }
        scopeSql = `(
          r.created_by_user_id = $1
          OR r.assigned_to_user_id = $2
          OR r.meeting_id IN (SELECT id FROM meetings WHERE assigned_to_user_id = $3 OR created_by_user_id = $4)
          ${legacyCreated}
          ${legacyAssigned}
          ${legacyMeetings}
        )`;
      }
    } else {
      // Everyone aggregate: OWNER/ADMIN only; EMPLOYEE forbidden
      if (!(role === 'OWNER' || role === 'ADMIN')) return res.status(403).json({ error: 'Forbidden' });
      scopeSql = ''; // no per-user restriction
    }

  // Dates
  const nowTs = formatLocalSQL(new Date());
  const startToday = localStartOfDayPlus(0);
    const startTomorrow = localStartOfDayPlus(1);
    const startDayAfter = localStartOfDayPlus(2);
    const tomorrow = new Date();
    tomorrow.setDate(tomorrow.getDate() + 1);
    tomorrow.setHours(0,0,0,0);
    const startWeek = startOfWeekFor(tomorrow);
    const startWeekDate = toLocalDate(startWeek);
    const plusDays = (d, n) => { const x = new Date(d.getTime()); x.setDate(x.getDate() + n); return x; };
    const startNextWeekTs = formatLocalSQL(plusDays(startWeekDate, 7));
    const startMonthTs = startNextWeekTs;
    const startNextMonthTs = formatLocalSQL(plusDays(startWeekDate, 28));

  const dateParams = [nowTs, startToday, startTomorrow, startDayAfter, startWeek, startNextWeekTs, startMonthTs, startNextMonthTs];
    const allParams = params.concat(dateParams);

    const whereScope = scopeSql ? `${scopeSql} AND` : '';
    const sql = `
      WITH scoped AS (
        SELECT r.status, r.due_ts
          FROM reminders r
         WHERE ${whereScope}
               r.type IN ('CALL','EMAIL')
      )
      SELECT
        COUNT(*) FILTER (WHERE status='PENDING' AND due_ts < LOCALTIMESTAMP)::int AS overdue_global,
        COUNT(*) FILTER (WHERE status='PENDING' AND due_ts >= LOCALTIMESTAMP)::int AS pending_total,
        COUNT(*) FILTER (WHERE status='DONE')::int AS done_total,
        COUNT(*) FILTER (WHERE status='SENT')::int AS sent_total,
        COUNT(*) FILTER (WHERE status='FAILED')::int AS failed_total,
        COUNT(*) FILTER (WHERE status='PENDING' AND due_ts >= $${params.length + 2} AND due_ts < $${params.length + 3} AND due_ts >= LOCALTIMESTAMP)::int AS pending_today,
        COUNT(*) FILTER (WHERE status='DONE' AND due_ts >= $${params.length + 2} AND due_ts < $${params.length + 3})::int AS done_today,
        COUNT(*) FILTER (WHERE status='SENT' AND due_ts >= $${params.length + 2} AND due_ts < $${params.length + 3})::int AS sent_today,
        COUNT(*) FILTER (WHERE status='FAILED' AND due_ts >= $${params.length + 2} AND due_ts < $${params.length + 3})::int AS failed_today,
        COUNT(*) FILTER (WHERE status='PENDING' AND due_ts >= $${params.length + 3} AND due_ts < $${params.length + 4})::int AS pending_tomorrow,
        COUNT(*) FILTER (WHERE status='DONE' AND due_ts >= $${params.length + 3} AND due_ts < $${params.length + 4})::int AS done_tomorrow,
        COUNT(*) FILTER (WHERE status='SENT' AND due_ts >= $${params.length + 3} AND due_ts < $${params.length + 4})::int AS sent_tomorrow,
        COUNT(*) FILTER (WHERE status='FAILED' AND due_ts >= $${params.length + 3} AND due_ts < $${params.length + 4})::int AS failed_tomorrow,
        COUNT(*) FILTER (WHERE status='PENDING' AND due_ts >= $${params.length + 5} AND due_ts < $${params.length + 6})::int AS pending_week,
        COUNT(*) FILTER (WHERE status='DONE' AND due_ts >= $${params.length + 5} AND due_ts < $${params.length + 6})::int AS done_week,
        COUNT(*) FILTER (WHERE status='SENT' AND due_ts >= $${params.length + 5} AND due_ts < $${params.length + 6})::int AS sent_week,
        COUNT(*) FILTER (WHERE status='FAILED' AND due_ts >= $${params.length + 5} AND due_ts < $${params.length + 6})::int AS failed_week,
        COUNT(*) FILTER (WHERE status='PENDING' AND due_ts >= $${params.length + 7} AND due_ts < $${params.length + 8})::int AS pending_month,
        COUNT(*) FILTER (WHERE status='DONE' AND due_ts >= $${params.length + 7} AND due_ts < $${params.length + 8})::int AS done_month,
        COUNT(*) FILTER (WHERE status='SENT' AND due_ts >= $${params.length + 7} AND due_ts < $${params.length + 8})::int AS sent_month,
        COUNT(*) FILTER (WHERE status='FAILED' AND due_ts >= $${params.length + 7} AND due_ts < $${params.length + 8})::int AS failed_month
      FROM scoped
    `;
    const q = await pool.query(sql, allParams);
    const row = q.rows[0] || {};
    const overdueGlobal = Number(row.overdue_global || 0);
    const result = {
      total: { overdue: overdueGlobal, pending: Number(row.pending_total || 0), done: Number(row.done_total || 0), sent: Number(row.sent_total || 0), failed: Number(row.failed_total || 0) },
      today: { overdue: overdueGlobal, pending: Number(row.pending_today || 0), done: Number(row.done_today || 0), sent: Number(row.sent_today || 0), failed: Number(row.failed_today || 0) },
      tomorrow: { overdue: 0, pending: Number(row.pending_tomorrow || 0), done: Number(row.done_tomorrow || 0), sent: Number(row.sent_tomorrow || 0), failed: Number(row.failed_tomorrow || 0) },
      week: { overdue: 0, pending: Number(row.pending_week || 0), done: Number(row.done_week || 0), sent: Number(row.sent_week || 0), failed: Number(row.failed_week || 0) },
      month: { overdue: 0, pending: Number(row.pending_month || 0), done: Number(row.done_month || 0), sent: Number(row.sent_month || 0), failed: Number(row.failed_month || 0) },
      generatedAt: new Date().toISOString()
    };
    res.json(result);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// Diagnostics: email configuration status (no secrets)
app.get('/api/diagnostics/email-config', (req, res) => {
  try {
    const cfg = {
      host: process.env.SMTP_HOST || (process.env.SMTP_USER && /@gmail\.com$/i.test(String(process.env.SMTP_USER)) ? 'smtp.gmail.com' : undefined),
      port: Number(process.env.SMTP_PORT || (process.env.SMTP_USER && /@gmail\.com$/i.test(String(process.env.SMTP_USER)) ? 465 : 587)),
      secure: !!(String(process.env.SMTP_SECURE || (process.env.SMTP_USER && /@gmail\.com$/i.test(String(process.env.SMTP_USER)) ? 'true' : 'false')).match(/^(1|true|yes|on)$/i)),
      userPresent: !!process.env.SMTP_USER,
      passPresent: !!process.env.SMTP_PASS,
      from: process.env.MAIL_FROM || process.env.SMTP_USER || null,
      apiOrigin: process.env.API_ORIGIN || null,
      provider: process.env.SENDGRID_API_KEY ? 'sendgrid-api' : 'smtp'
    };
    const missing = [];
    if (!cfg.host) missing.push('SMTP_HOST');
    if (!cfg.userPresent) missing.push('SMTP_USER');
    if (!cfg.passPresent) missing.push('SMTP_PASS');
    res.json({ configured: missing.length === 0, missing, configPreview: cfg });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Diagnostics: verify SMTP connectivity/auth (no secrets in response)
app.get('/api/diagnostics/email-verify', async (req, res) => {
  try {
    const r = await verifySmtp();
    res.json({ ok: true, verified: !!r.ok });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message, code: e.code || null });
  }
});

// Diagnostics: summarize schema presence (no auth)
app.get('/api/diagnostics/schema-summary', async (req, res) => {
  try {
    const cols = await pool.query(`
      SELECT column_name
        FROM information_schema.columns
       WHERE table_schema='public' AND table_name='opportunities'
         AND column_name IN ('sector','location_url')
       ORDER BY column_name`);
    const img = await pool.query(`SELECT to_regclass('public.opportunity_images') AS reg`);
    res.json({
      featureFlags,
      opportunitiesColumns: cols.rows.map(r => r.column_name),
      opportunityImagesTable: !!(img.rows && img.rows[0] && img.rows[0].reg)
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Admin endpoint to ensure schema parts exist now
app.post('/api/admin/ensure-schema', requireAuth, requireRole('OWNER','ADMIN'), async (req, res) => {
  try {
    await ensureMinimalSchema(pool);
    await refreshFeatureFlags(pool);
    // Return snapshot of current columns and table presence to confirm
    const cols = await pool.query(`
      SELECT column_name
        FROM information_schema.columns
       WHERE table_schema='public' AND table_name='opportunities'
         AND column_name IN ('sector','location_url')
       ORDER BY column_name`);
    const img = await pool.query(`SELECT to_regclass('public.opportunity_images') AS reg`);
    res.json({
      ok: true,
      featureFlags,
      opportunitiesColumns: cols.rows.map(r => r.column_name),
      opportunityImagesTable: !!(img.rows && img.rows[0] && img.rows[0].reg)
    });
  } catch (e) {
    res.status(500).json({ ok:false, error: e.message });
  }
});
// ------------ Common validators -------------
function clampInt(val, min, max, def) {
  const n = Number(val);
  if (!Number.isFinite(n)) return def;
  return Math.min(Math.max(Math.trunc(n), min), max);
}
// Normalize assignment field to canonical values: 'CUSTOMER' | 'CONTRACT' | null
function normalizeAssignment(val) {
  if (!val) return null;
  const v = String(val).trim().toUpperCase();
  if (v === 'CUSTOMER' || v === 'CONTRACT') return v;
  return null;
}
function safeList(list, allowed) {
  const a = new Set(allowed.map(x => String(x).toUpperCase()));
  return (list || []).map(x => String(x).toUpperCase()).filter(x => a.has(x));
}
function isValidDateTimeString(s) {
  if (!s || typeof s !== 'string') return false;
  // Accept YYYY-MM-DD HH:mm[:ss] or ISO-like
  if (/^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?$/.test(s)) return true;
  // Accept DD-MM-YYYY HH:mm[:ss]
  if (/^\d{2}-\d{2}-\d{4}(?:[ T]\d{2}:\d{2}(?::\d{2})?)?$/.test(s)) return true;
  // Accept a parseable ISO as a fallback
  const d = new Date(s);
  return !isNaN(d.getTime());
}
// Format a Date as local-time SQL timestamp (YYYY-MM-DD HH:mm:ss) without TZ conversion
function fmtSqlTsLocal(d) {
  const x = new Date(d);
  const pad = n => String(n).padStart(2, '0');
  return `${x.getFullYear()}-${pad(x.getMonth()+1)}-${pad(x.getDate())} ${pad(x.getHours())}:${pad(x.getMinutes())}:${pad(x.getSeconds())}`;
}
// Coerce various user-entered date/time strings into a local SQL timestamp string.
// Rules:
// - If input is 'YYYY-MM-DD HH:mm[:ss]' or 'YYYY-MM-DDTHH:mm[:ss]', keep the local wall clock time as-entered.
// - If input is 'DD-MM-YYYY HH:mm[:ss]' (or with 'T'), convert to YYYY-MM-DD and keep local time.
// - If input contains explicit TZ (Z/+hh:mm), parse and then format as local wall time.
function coerceLocalSqlTimestamp(input) {
  if (!input) return null;
  let s = String(input).trim();
  if (!s) return null;
  // Normalize T to space for simple patterns
  s = s.replace('T', ' ');
  // DD-MM-YYYY [HH:mm[:ss]]
  let m = s.match(/^(\d{2})-(\d{2})-(\d{4})(?: (\d{2}):(\d{2})(?::(\d{2}))?)?$/);
  if (m) {
    const [, dd, mm, yyyy, HH='00', MM='00', SS='00'] = m;
    return `${yyyy}-${mm}-${dd} ${HH}:${MM}:${SS}`;
  }
  // YYYY-MM-DD [HH:mm[:ss]] (no timezone) -> keep local wall time
  m = s.match(/^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2})(?::(\d{2}))?$/);
  if (m) {
    const [, yyyy, mm, dd, HH, MM, SS='00'] = m;
    return `${yyyy}-${mm}-${dd} ${HH}:${MM}:${SS}`;
  }
  // If string contains explicit timezone (Z or +hh:mm), parse and render in local
  if (/Z|[+-]\d{2}:\d{2}$/.test(s)) {
    const d = new Date(s);
    if (!isNaN(d.getTime())) return fmtSqlTsLocal(d);
  }
  // Fallback: try Date parse and render as local
  const d = new Date(s);
  if (!isNaN(d.getTime())) return fmtSqlTsLocal(d);
  return null;
}
async function validateSelectableUserId(db, userId) {
  if (!userId) return null;
  // Allow OWNER, EMPLOYEE, and ADMIN as selectable (needed for Admin "My Overview")
  const r = await db.query(`SELECT id FROM public.users WHERE id=$1 AND active=TRUE AND role IN ('OWNER','EMPLOYEE','ADMIN')`, [userId]);
  return r.rows.length ? r.rows[0].id : null;
}
// Example route to test DB connection
// Remove or modify as needed
app.get('/api/test-db', async (req, res) => {
  try {
    const result = await pool.query('SELECT NOW()');
    res.json({ time: result.rows[0].now });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// --- Targets CRUD ---
app.get('/api/targets', requireAuth, async (req, res) => {
  try {
    const { status, q, page = 1, pageSize = 50 } = req.query;
    const p = Math.max(Number(page) || 1, 1);
    const s = Math.min(Math.max(Number(pageSize) || 50, 1), 200);
    const offset = (p - 1) * s;
    const params = [];
    const filters = [];
    if (status) {
      const list = String(status).split(',').map(x => x.trim().toUpperCase()).filter(Boolean);
      if (list.length) {
        const ph = list.map((_, i) => `$${params.length + i + 1}`).join(',');
        params.push(...list);
        filters.push(`t.status IN (${ph})`);
      }
    }
    if (q) {
      params.push(`%${String(q).toLowerCase()}%`);
      filters.push(`LOWER(t.client_name) LIKE $${params.length}`);
    }
    const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';
    const sql = `
      SELECT t.*, ua.full_name AS assigned_full_name, ua.username AS assigned_username, ua.email AS assigned_email
      FROM targets t
      LEFT JOIN public.users ua ON ua.id = t.assigned_to_user_id
      ${where}
      ORDER BY (t.status = 'PENDING') DESC, t.updated_at DESC
      LIMIT ${s} OFFSET ${offset}
    `;
    const r = await pool.query(sql, params);
    res.json({ items: r.rows, page: p, pageSize: s });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/targets', requireAuth, requireRole('OWNER','ADMIN','EMPLOYEE'), async (req, res) => {
  try {
    let { id, client_name, notes, status, assignedToUserId, assigned_to } = req.body || {};
    client_name = (client_name || '').toString().trim();
    if (!client_name) return res.status(400).json({ error: 'client_name is required' });
    status = String(status || 'PENDING').toUpperCase();
    const allowedStatuses = new Set(['PENDING','DONE','COMPETITOR','ON_HOLD','CANCELLED','DUPLICATE','FOLLOW_UP']);
    if (!allowedStatuses.has(status)) return res.status(400).json({ error: 'Invalid status' });
    if (!id) id = Math.random().toString(36).slice(2, 10).toUpperCase();
    // Resolve assignment: employee can only assign to self
    let assignedUserId = null;
    let assignedLabel = null;
    const role = (req.user && req.user.role) || 'EMPLOYEE';
    const actor = getActor(req);
    const actorUserId = req.user && req.user.sub;
    if (role === 'EMPLOYEE') {
      assignedUserId = actorUserId;
      const rr = await pool.query('SELECT email, username, full_name FROM public.users WHERE id=$1', [actorUserId]);
      assignedLabel = rr.rows.length ? pickDisplay(rr.rows[0]) : actor;
    } else {
      if (assignedToUserId) {
        const valid = await validateSelectableUserId(pool, String(assignedToUserId));
        if (!valid) return res.status(400).json({ error: 'Invalid assignedToUserId' });
        assignedUserId = valid;
        const rr = await pool.query('SELECT email, username, full_name FROM public.users WHERE id=$1', [valid]);
        assignedLabel = rr.rows.length ? pickDisplay(rr.rows[0]) : null;
      } else if (assigned_to) {
        const u = await resolveUserByIdentifier(assigned_to);
        if (u) { assignedUserId = u.id; assignedLabel = pickDisplay(u); }
      }
      if (!assignedUserId) {
        assignedUserId = actorUserId;
        const rr = await pool.query('SELECT email, username, full_name FROM public.users WHERE id=$1', [actorUserId]);
        assignedLabel = rr.rows.length ? pickDisplay(rr.rows[0]) : actor;
      }
    }
    const sql = `INSERT INTO targets (id, client_name, notes, status, assigned_to, assigned_to_user_id, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,NOW(),NOW()) RETURNING *`;
    const r = await pool.query(sql, [id, client_name, notes || null, status, assignedLabel, assignedUserId]);
    res.status(201).json(r.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.put('/api/targets/:id', requireAuth, requireRole('OWNER','ADMIN','EMPLOYEE'), async (req, res) => {
  try {
    const id = req.params.id;
    const cur = await pool.query('SELECT * FROM targets WHERE id=$1', [id]);
    if (cur.rows.length === 0) return res.status(404).json({ error: 'Not found' });
    let { client_name, notes, status, assignedToUserId, assigned_to } = req.body || {};
    const newName = client_name !== undefined ? String(client_name || '').trim() : cur.rows[0].client_name;
    if (!newName) return res.status(400).json({ error: 'client_name is required' });
    let newStatus = status !== undefined ? String(status || '').toUpperCase() : cur.rows[0].status;
    const allowedStatuses = new Set(['PENDING','DONE','COMPETITOR','ON_HOLD','CANCELLED','DUPLICATE','FOLLOW_UP']);
    if (!allowedStatuses.has(newStatus)) return res.status(400).json({ error: 'Invalid status' });
    const newNotes = notes !== undefined ? (notes || null) : cur.rows[0].notes;
    // Resolve assignment if provided; employees cannot reassign away from themselves
    let assignedUserId = cur.rows[0].assigned_to_user_id;
    let assignedLabel = cur.rows[0].assigned_to;
    const role = (req.user && req.user.role) || 'EMPLOYEE';
    const actor = getActor(req);
    const actorUserId = req.user && req.user.sub;
    if (role === 'EMPLOYEE') {
      assignedUserId = actorUserId;
      const rr = await pool.query('SELECT email, username, full_name FROM public.users WHERE id=$1', [actorUserId]);
      assignedLabel = rr.rows.length ? pickDisplay(rr.rows[0]) : actor;
    } else {
      if (assignedToUserId) {
        const valid = await validateSelectableUserId(pool, String(assignedToUserId));
        if (!valid) return res.status(400).json({ error: 'Invalid assignedToUserId' });
        assignedUserId = valid;
        const rr = await pool.query('SELECT email, username, full_name FROM public.users WHERE id=$1', [valid]);
        assignedLabel = rr.rows.length ? pickDisplay(rr.rows[0]) : null;
      } else if (assigned_to !== undefined) {
        const u = assigned_to ? await resolveUserByIdentifier(assigned_to) : null;
        if (u) { assignedUserId = u.id; assignedLabel = pickDisplay(u); }
      }
    }
    const upd = await pool.query('UPDATE targets SET client_name=$1, notes=$2, status=$3, assigned_to=$4, assigned_to_user_id=$5, updated_at=NOW() WHERE id=$6 RETURNING *', [newName, newNotes, newStatus, assignedLabel, assignedUserId, id]);
    res.json(upd.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.patch('/api/targets/:id/status', requireAuth, requireRole('OWNER','ADMIN','EMPLOYEE'), async (req, res) => {
  try {
    const id = req.params.id;
    let { status } = req.body || {};
    status = String(status || '').toUpperCase();
    if (!['PENDING','DONE'].includes(status)) return res.status(400).json({ error: 'Invalid status' });
    const r = await pool.query('UPDATE targets SET status=$1, updated_at=NOW() WHERE id=$2 RETURNING *', [status, id]);
    if (r.rows.length === 0) return res.status(404).json({ error: 'Not found' });
    res.json(r.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/targets/:id', requireAuth, requireRole('OWNER','ADMIN'), async (req, res) => {
  try {
    const id = req.params.id;
    const r = await pool.query('DELETE FROM targets WHERE id=$1 RETURNING *', [id]);
    if (r.rows.length === 0) return res.status(404).json({ error: 'Not found' });
    res.json({ success: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// --- Customers CRUD ---
// Small helper to send cacheable JSON with ETag and short-lived cache
function sendCacheableJson(req, res, body) {
  try {
    const payload = typeof body === 'string' ? body : JSON.stringify(body);
    // a simple weak etag: length-hash
    let hash = 0;
    for (let i = 0; i < payload.length; i++) {
      hash = (hash * 31 + payload.charCodeAt(i)) >>> 0;
    }
    const etag = 'W/"' + payload.length + '-' + hash.toString(16) + '"';
  const inm = ((req && req.headers && req.headers['if-none-match']) || '').trim();
    res.setHeader('ETag', etag);
    res.setHeader('Cache-Control', 'public, max-age=30, must-revalidate');
    if (inm && inm === etag) {
      return res.status(304).end();
    }
    res.type('application/json');
    return res.send(payload);
  } catch {
    return res.json(body);
  }
}

app.get('/api/customers', async (req, res) => {
  try {
    const { q, page = 1, pageSize = 100 } = req.query;
    const p = clampInt(page, 1, 100000, 1);
    const s = clampInt(pageSize, 1, 500, 100);
    const offset = (p - 1) * s;
    const params = [];
    const filters = ["o.stage = 'AGREED'", "c.customer_status = 'ACTIVE'"];
    if (q) {
      params.push(`%${String(q).toLowerCase()}%`);
      const idx = params.length;
      filters.push(`(LOWER(c.client_name) LIKE $${idx} OR LOWER(c.customer_id) LIKE $${idx})`);
    }
    const where = `WHERE ${filters.join(' AND ')}`;
    const sql = `
      SELECT c.*
      FROM customers c
      JOIN opportunities o ON o.opportunity_id = c.opportunity_id
      ${where}
      ORDER BY c.created_at DESC
      LIMIT ${s} OFFSET ${offset}
    `;
    const rows = await pool.query(sql, params);
    const cnt = await pool.query(`SELECT COUNT(*)::int AS n FROM customers c JOIN opportunities o ON o.opportunity_id=c.opportunity_id ${where}`, params);
    return sendCacheableJson(req, res, { items: rows.rows, total: cnt.rows[0].n, page: p, pageSize: s });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});
app.get('/api/customers/:customer_id', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM customers WHERE customer_id = $1', [req.params.customer_id]);
    if (result.rows.length === 0) return res.status(404).json({ error: 'Not found' });
    res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// Helper to generate 6-char alphanumeric customer_id
function generateCustomerId() {
  return Math.random().toString(36).substr(2, 6).toUpperCase();
}

app.post('/api/customers', async (req, res) => {
  const { opportunity_id, client_name, gstin, primary_contact, phone, alt_phone, email } = req.body;
  const customer_id = generateCustomerId();
  try {
    const result = await pool.query(
      'INSERT INTO customers (customer_id, opportunity_id, client_name, gstin, primary_contact, phone, alt_phone, email, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW()) RETURNING *',
      [customer_id, opportunity_id, client_name, gstin, primary_contact, phone, alt_phone, email]
    );
    res.status(201).json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.put('/api/customers/:customer_id', async (req, res) => {
  const { client_name, gstin, primary_contact, phone, alt_phone, email } = req.body;
  try {
    const result = await pool.query(
      'UPDATE customers SET client_name=$1, gstin=$2, primary_contact=$3, phone=$4, alt_phone=$5, email=$6 WHERE customer_id=$7 RETURNING *',
      [client_name, gstin, primary_contact, phone, alt_phone, email, req.params.customer_id]
    );
    if (result.rows.length === 0) return res.status(404).json({ error: 'Not found' });
    res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/customers/:customer_id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM customers WHERE customer_id = $1 RETURNING *', [req.params.customer_id]);
    if (result.rows.length === 0) return res.status(404).json({ error: 'Not found' });
    res.json({ success: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});
// --- Contracts CRUD ---
// --- Contracts CRUD (updated schema) ---
app.get('/api/contracts', async (req, res) => {
  try {
    const { q, page = 1, pageSize = 100 } = req.query;
    const p = clampInt(page, 1, 100000, 1);
    const s = clampInt(pageSize, 1, 500, 100);
    const offset = (p - 1) * s;
    const params = [];
    const filters = ["o.stage = 'AGREED'", "c.contract_status = 'ACTIVE'"];
    if (q) {
      params.push(`%${String(q).toLowerCase()}%`);
      const idx = params.length;
      filters.push(`(LOWER(c.client_name) LIKE $${idx} OR LOWER(c.contract_id) LIKE $${idx})`);
    }
    const where = `WHERE ${filters.join(' AND ')}`;
    const sql = `
      SELECT c.*
      FROM contracts c
      JOIN opportunities o ON o.opportunity_id = c.opportunity_id
      ${where}
      ORDER BY c.created_at DESC
      LIMIT ${s} OFFSET ${offset}
    `;
    const rows = await pool.query(sql, params);
    const cnt = await pool.query(`SELECT COUNT(*)::int AS n FROM contracts c JOIN opportunities o ON o.opportunity_id=c.opportunity_id ${where}`, params);
    return sendCacheableJson(req, res, { items: rows.rows, total: cnt.rows[0].n, page: p, pageSize: s });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/contracts/:contract_id', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM contracts WHERE contract_id = $1', [req.params.contract_id]);
    if (result.rows.length === 0) return res.status(404).json({ error: 'Not found' });
    res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/contracts', async (req, res) => {
  const { contract_id, opportunity_id, client_name, quoted_price_per_litre, start_date, end_date, primary_contact, credit_period, phone_number, alt_phone, gstin, email } = req.body;
  try {
    const result = await pool.query(
      'INSERT INTO contracts (contract_id, opportunity_id, client_name, quoted_price_per_litre, start_date, end_date, primary_contact, credit_period, phone_number, alt_phone, gstin, email, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW()) RETURNING *',
      [contract_id, opportunity_id, client_name, quoted_price_per_litre, start_date, end_date, primary_contact, credit_period, phone_number, alt_phone, gstin, email]
    );
    res.status(201).json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.put('/api/contracts/:contract_id', async (req, res) => {
  const { opportunity_id, client_name, quoted_price_per_litre, start_date, end_date, primary_contact, credit_period, phone_number, alt_phone, gstin, email } = req.body;
  try {
    const result = await pool.query(
      'UPDATE contracts SET opportunity_id=$1, client_name=$2, quoted_price_per_litre=$3, start_date=$4, end_date=$5, primary_contact=$6, credit_period=$7, phone_number=$8, alt_phone=$9, gstin=$10, email=$11 WHERE contract_id=$12 RETURNING *',
      [opportunity_id, client_name, quoted_price_per_litre, start_date, end_date, primary_contact, credit_period, phone_number, alt_phone, gstin, email, req.params.contract_id]
    );
    if (result.rows.length === 0) return res.status(404).json({ error: 'Not found' });
    res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/contracts/:contract_id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM contracts WHERE contract_id = $1 RETURNING *', [req.params.contract_id]);
    if (result.rows.length === 0) return res.status(404).json({ error: 'Not found' });
    res.json({ success: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// --- Opportunities CRUD ---
// Shared profile builder used by multiple endpoints; applies role-based scoping using req.user
async function getClientProfileByOpportunityId(opportunityId, pool, req) {
  // Refresh feature flags to align with the live DB schema for this request
  try { await refreshFeatureFlags(pool); } catch {}
  const oppId = String(opportunityId || '').trim();
  if (!oppId) throw new Error('opportunityId is required');

  // Base opportunity with spend aggregate
  const oppSql = `
      SELECT
        ${buildOpportunitySelectFields(true).join(',\n        ')}
      FROM opportunities o
      LEFT JOIN (
        SELECT opportunity_id, SUM(amount) AS total
        FROM expenses
        WHERE status = 'ACTIVE'
        GROUP BY opportunity_id
      ) x ON x.opportunity_id = o.opportunity_id
      WHERE o.opportunity_id = $1
      LIMIT 1
    `;
  const oppRes = await pool.query(oppSql, [oppId]);
  if (!oppRes.rows.length) throw new Error('Opportunity not found');
  const opportunity = oppRes.rows[0];

  // Customer and Contract (best/active)
  const custSql = `
      SELECT customer_id, client_name, gstin, primary_contact, phone, alt_phone, email, created_at, customer_status
      FROM customers
      WHERE opportunity_id=$1
      ORDER BY (customer_status='ACTIVE') DESC, created_at DESC
      LIMIT 1
    `;
  const contSql = `
      SELECT contract_id, opportunity_id, client_name, gstin, start_date, end_date, primary_contact, phone_number, alt_phone, email,
             quoted_price_per_litre, credit_period, contract_status, created_at
      FROM contracts
      WHERE opportunity_id=$1
      ORDER BY (contract_status='ACTIVE') DESC, created_at DESC
      LIMIT 1
    `;
  const [custRes, contRes] = await Promise.all([
    pool.query(custSql, [oppId]),
    pool.query(contSql, [oppId])
  ]);
  const customer = custRes.rows[0] || null;
  const contract = contRes.rows[0] || null;

  // Upcoming 7 days (including today) local range
  const start = new Date(); start.setHours(0,0,0,0);
  const end = new Date(start.getTime()); end.setDate(end.getDate()+7); end.setHours(23,59,59,999);
  const from = formatLocalSQL(start);
  const to = formatLocalSQL(end);

  // Role-aware scoping: OWNER/ADMIN see all; EMPLOYEE sees only meetings they created/are assigned to, and reminders they created or linked via their meetings
  const isOwnerAdmin = req && req.user && (req.user.role === 'OWNER' || req.user.role === 'ADMIN');
  const userId = req && req.user && req.user.sub;
  const actor = getActor(req);

  let meetingsSql = `
      SELECT m.id, m.subject, m.starts_at, m.status, m.location, m.assigned_to, m.assigned_to_user_id, m.created_by, m.created_by_user_id
        FROM meetings m
       WHERE m.opportunity_id=$1
         AND m.status IN ('SCHEDULED','RESCHEDULED')
         AND m.starts_at BETWEEN $2 AND $3
    `;
  const mParams = [oppId, from, to];
  if (!isOwnerAdmin) {
    meetingsSql += ` AND (m.assigned_to_user_id = $4 OR m.created_by_user_id = $4 OR m.assigned_to = $5 OR m.created_by = $5)`;
    mParams.push(userId, actor);
  }
  meetingsSql += ` ORDER BY m.starts_at ASC NULLS LAST LIMIT 200`;

  let remindersSql = `
      SELECT r.id, r.title, r.type, r.status, r.due_ts, r.created_by, r.created_by_user_id, r.meeting_id,
             COALESCE(r.assigned_to, m.assigned_to) AS assigned_to,
             COALESCE(r.assigned_to_user_id, m.assigned_to_user_id) AS assigned_to_user_id
        FROM reminders r
        LEFT JOIN meetings m ON m.id = r.meeting_id
       WHERE r.opportunity_id=$1
         AND r.type IN ('CALL','EMAIL')
         AND r.status='PENDING'
         AND r.due_ts BETWEEN $2 AND $3
    `;
  const rParams = [oppId, from, to];
  if (!isOwnerAdmin) {
    remindersSql += ` AND (
        r.created_by_user_id = $4 OR r.created_by = $5 OR
        r.meeting_id IN (
          SELECT id FROM meetings m2 WHERE (m2.assigned_to_user_id = $4 OR m2.created_by_user_id = $4 OR m2.assigned_to = $5 OR m2.created_by = $5)
        )
      )`;
    rParams.push(userId, actor);
  }
  remindersSql += ` ORDER BY r.due_ts ASC NULLS LAST LIMIT 200`;

  const [mRes, rRes] = await Promise.all([
    pool.query(meetingsSql, mParams),
    pool.query(remindersSql, rParams)
  ]);

  const details = {
    client_name: opportunity.client_name || (customer && customer.client_name) || (contract && contract.client_name) || null,
    primary_contact: (customer && customer.primary_contact) || (contract && contract.primary_contact) || null,
    phone: (customer && (customer.phone || customer.alt_phone)) || (contract && contract.phone_number) || null,
    email: (customer && customer.email) || (contract && contract.email) || null,
    salesperson: opportunity.salesperson || null,
  };

  const assignmentNorm = normalizeAssignment(opportunity.assignment);
  let kind = customer ? 'CUSTOMER' : (contract ? 'CONTRACT' : (assignmentNorm || 'CUSTOMER'));
  let rightPanel = { kind };
  if (kind === 'CUSTOMER') {
    rightPanel.customer = customer ? {
      customer_id: customer.customer_id,
      gstin: customer.gstin || null,
      expected_monthly_volume_l: opportunity.expected_monthly_volume_l || null,
      customer_status: customer.customer_status || null
    } : null;
  } else if (kind === 'CONTRACT') {
    rightPanel.contract = contract ? {
      contract_id: contract.contract_id,
      client_name: contract.client_name || opportunity.client_name || null,
      gstin: contract.gstin || null,
      start_date: contract.start_date || null,
      end_date: contract.end_date || null,
      primary_contact: contract.primary_contact || null,
      phone_number: contract.phone_number || null,
      alt_phone: contract.alt_phone || null,
      email: contract.email || null,
      quoted_price_per_litre: contract.quoted_price_per_litre || null,
      credit_period: contract.credit_period || null,
      contract_status: contract.contract_status || null
    } : null;
  }

  return { opportunity, details, rightPanel, meetings: mRes.rows, reminders: rRes.rows };
}

// Unified client profile resolver for opportunity, customer, contract
async function getUnifiedClientProfile(opportunityId, pool, req) {
  return await getClientProfileByOpportunityId(opportunityId, pool, req);
}

// Resolve Opportunity ID from Customer ID
app.get('/api/client-profile/by-customer/:customerId', requireAuth, async (req, res) => {
  const { customerId } = req.params;
  try {
    // Resolve the linked opportunity directly from the customers table
    const { rows } = await pool.query(
      `SELECT opportunity_id FROM customers WHERE customer_id = $1 LIMIT 1`,
      [customerId]
    );
    if (!rows.length) {
      return res.status(404).json({ error: 'No opportunity found for this customer' });
    }
    const opportunityId = rows[0].opportunity_id;
    const profile = await getUnifiedClientProfile(opportunityId, pool, req);
    // Always include the resolved opportunityId in the payload
    res.json({ ...profile, resolvedOpportunityId: opportunityId });
  } catch (e) {
    res.status(500).json({ error: e.message || String(e) });
  }
});

// Resolve Opportunity ID from Contract ID
app.get('/api/client-profile/by-contract/:contractId', requireAuth, async (req, res) => {
  const { contractId } = req.params;
  try {
    // Find the linked opportunity for this contract
    const { rows } = await pool.query(
      `SELECT opportunity_id FROM contracts WHERE contract_id = $1 LIMIT 1`,
      [contractId]
    );
    if (!rows.length) {
      return res.status(404).json({ error: 'No opportunity found for this contract' });
    }
    const opportunityId = rows[0].opportunity_id;
  const profile = await getUnifiedClientProfile(opportunityId, pool, req);
    res.json({ ...profile, resolvedOpportunityId: opportunityId });
  } catch (e) {
    res.status(500).json({ error: e.message || String(e) });
  }
});

// --- Opportunities CRUD ---
app.get('/api/opportunities', async (req, res) => {
  try {
    await refreshFeatureFlags(pool);
    const { q, salesperson, sector, stage, page = 1, pageSize = 50, sort = 'client_name_asc' } = req.query;
    const p = clampInt(page, 1, 100000, 1);
    const s = clampInt(pageSize, 1, 200, 50);
    const offset = (p - 1) * s;
    const filters = [];
    const params = [];
    if (q) {
      params.push(`%${String(q).toLowerCase()}%`);
      const idx = params.length;
      filters.push(`(LOWER(o.client_name) LIKE $${idx} OR LOWER(o.opportunity_id) LIKE $${idx} OR LOWER(COALESCE(o.purpose,'')) LIKE $${idx})`);
    }
    if (salesperson) { params.push(salesperson); filters.push(`o.salesperson = $${params.length}`); }
    if (sector) { params.push(sector); filters.push(`o.sector = $${params.length}`); }
    if (stage) { params.push(stage); filters.push(`o.stage = $${params.length}`); }
    const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';
    const sortMap = {
      client_name_asc: 'o.client_name ASC',
      client_name_desc: 'o.client_name DESC',
      opportunity_id_asc: 'o.opportunity_id ASC',
      opportunity_id_desc: 'o.opportunity_id DESC',
      spend_desc: 'COALESCE(x.total,0) DESC',
      spend_asc: 'COALESCE(x.total,0) ASC',
      stage_asc: 'o.stage ASC',
      stage_desc: 'o.stage DESC',
      price_asc: 'o.proposed_price_per_litre ASC NULLS LAST',
      price_desc: 'o.proposed_price_per_litre DESC NULLS LAST',
      updated_desc: 'o.updated_at DESC NULLS LAST',
    };
    const orderBy = sortMap[String(sort).toLowerCase()] || sortMap.client_name_asc;
    const fields = buildOpportunitySelectFields(true).join(',\n        ');
    const sql = `
      SELECT ${fields}
        FROM opportunities o
        LEFT JOIN (
          SELECT opportunity_id, SUM(amount) AS total
            FROM expenses
           WHERE status = 'ACTIVE'
           GROUP BY opportunity_id
        ) x ON x.opportunity_id = o.opportunity_id
        ${where}
        ORDER BY ${orderBy}
        LIMIT ${s} OFFSET ${offset}
    `;
    const rows = await pool.query(sql, params);
    // Also fetch total for pagination
    const cnt = await pool.query(`SELECT COUNT(*)::int AS n FROM opportunities o ${where}`, params);
    return sendCacheableJson(req, res, { items: rows.rows, total: cnt.rows[0].n, page: p, pageSize: s });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});
app.get('/api/opportunities/:id', async (req, res) => {
  try {
    await refreshFeatureFlags(pool);
    const fields = buildOpportunitySelectFields(true).join(',\n        ');
    const sql = `
      SELECT
        ${fields}
      FROM opportunities o
      LEFT JOIN (
        SELECT opportunity_id, SUM(amount) AS total
        FROM expenses
        WHERE status = 'ACTIVE'
        GROUP BY opportunity_id
      ) x ON x.opportunity_id = o.opportunity_id
      WHERE o.opportunity_id = $1
    `;
    const result = await pool.query(sql, [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ error: 'Not found' });
    res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// Client Profile payload for an opportunity (OWNER/ADMIN)
// Includes: opportunity summary, related customer/contract info, upcoming 7-day meetings and reminders
app.get('/api/client-profile/:id', requireAuth, async (req, res) => {
  try {
    const oppId = String(req.params.id || '').trim();
    if (!oppId) return res.status(400).json({ error: 'opportunityId is required' });
    const payload = await getUnifiedClientProfile(oppId, pool, req);
    res.json(payload);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});
app.post('/api/opportunities', async (req, res) => {
  const { opportunity_id, client_name, purpose, expected_monthly_volume_l, proposed_price_per_litre, sector, location_url, stage, probability, notes, salesperson, assignment, spend, loss_reason } = req.body;
  try {
    await refreshFeatureFlags(pool);
    const fields = ['opportunity_id','client_name','purpose','expected_monthly_volume_l','proposed_price_per_litre','stage','probability','notes','salesperson','assignment','spend','loss_reason'];
    const values = [opportunity_id, client_name, purpose, expected_monthly_volume_l, proposed_price_per_litre, stage, probability, notes, salesperson, normalizeAssignment(assignment) || null, spend, loss_reason];
    if (featureFlags.hasSector) { fields.splice(5, 0, 'sector'); values.splice(5, 0, sector || null); } // before stage
    if (featureFlags.hasLocationUrl) {
      // insert location_url right after sector if present else after proposed_price_per_litre
      const insertIdx = featureFlags.hasSector ? 6 : 5;
      fields.splice(insertIdx, 0, 'location_url'); values.splice(insertIdx, 0, location_url || null);
    }
    const placeholders = values.map((_, i) => `$${i+1}`).join(',');
    const sql = `INSERT INTO opportunities (${fields.join(',')}) VALUES (${placeholders}) RETURNING *`;
    const result = await pool.query(sql, values);
    res.status(201).json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});
app.put('/api/opportunities/:id', requireAuth, async (req, res) => {
  const { client_name, purpose, expected_monthly_volume_l, proposed_price_per_litre, sector, location_url, stage, probability, notes, salesperson, assignment, spend, loss_reason } = req.body;
  const oppId = req.params.id;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // Lock current opportunity
    const currRes = await client.query('SELECT * FROM opportunities WHERE opportunity_id = $1 FOR UPDATE', [oppId]);
    if (currRes.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Not found' });
    }
    const current = currRes.rows[0];
    const fromStage = current.stage || 'LEAD';

    // Normalize assignment consistently (fallback to current when not provided)
    const assignmentNorm = normalizeAssignment(assignment !== undefined ? assignment : current.assignment) || null;

    // Update base fields
    await refreshFeatureFlags(pool);
    // Build dynamic SET list
    const sets = ['client_name=$1','purpose=$2','expected_monthly_volume_l=$3','proposed_price_per_litre=$4'];
    const params = [client_name, purpose, expected_monthly_volume_l, proposed_price_per_litre];
    let idx = params.length;
    if (featureFlags.hasSector) { idx++; sets.push(`sector=$${idx}`); params.push(sector || null); }
    if (featureFlags.hasLocationUrl) { idx++; sets.push(`location_url=$${idx}`); params.push(location_url || null); }
    idx++; sets.push(`stage=$${idx}`); params.push(stage);
    idx++; sets.push(`probability=$${idx}`); params.push(probability);
    idx++; sets.push(`notes=$${idx}`); params.push(notes);
    idx++; sets.push(`salesperson=$${idx}`); params.push(salesperson);
    idx++; sets.push(`assignment=$${idx}`); params.push(assignmentNorm);
    idx++; sets.push(`spend=$${idx}`); params.push(spend);
    idx++; sets.push(`loss_reason=$${idx}`); params.push(loss_reason);
    idx++; params.push(oppId);
    const sql = `UPDATE opportunities SET ${sets.join(', ')} WHERE opportunity_id=$${idx} RETURNING *`;
    const updateRes = await client.query(sql, params);
    const updatedOpp = updateRes.rows[0];

    const toStage = updatedOpp.stage || 'LEAD';
    const stageChanged = fromStage !== toStage;

    // Helper: allowed transitions and reason requirements
    function allowedTransitions(s) {
      const map = {
        LEAD: ['QUALIFIED', 'DISAGREED'],
        QUALIFIED: ['NEGOTIATION', 'DISAGREED'],
        NEGOTIATION: ['AGREED', 'DISAGREED'],
        AGREED: ['CANCELLED'],
        DISAGREED: ['LEAD', 'QUALIFIED', 'NEGOTIATION', 'AGREED'],
        CANCELLED: ['LEAD', 'QUALIFIED', 'NEGOTIATION', 'AGREED']
      };
      return map[s] || [];
    }
    const requiresReason = (fromStage, toStage) => {
      if (toStage === 'DISAGREED' || toStage === 'CANCELLED') return true;
      if ((fromStage === 'DISAGREED' || fromStage === 'CANCELLED') && ['LEAD','QUALIFIED','NEGOTIATION','AGREED'].includes(toStage)) return true; // reopen
      return false;
    };

  if (stageChanged) {
      // Validate transition (softly here to not break UI; we only log audit even if invalid)
      const allowed = allowedTransitions(fromStage);
      if (!allowed.includes(toStage)) {
        // Soft warning: we still proceed but log audit for traceability
        console.warn(`Invalid transition ${fromStage} -> ${toStage} for opportunity ${oppId}`);
      }

      // Determine reasons from request body; fallback to loss_reason for DISAGREED to stay backward-compatible
      const needReason = requiresReason(fromStage, toStage);
      let reason_code = null;
      let reason_text = null;
      if (needReason) {
        const rc = req.body.reasonCode;
        const rt = req.body.reasonText;
        if (rc) {
          reason_code = rc;
          reason_text = rt || null;
        } else if (toStage === 'DISAGREED' && (loss_reason && loss_reason.trim().length)) {
          reason_code = 'other';
          reason_text = loss_reason;
        } else {
          // Enforce reason for required transitions
          await client.query('ROLLBACK');
          return res.status(400).json({ error: 'reasonCode is required for this transition' });
        }
      }

      const actor = getActor(req);
      await client.query(
        'INSERT INTO opportunity_stage_audit (opportunity_id, from_stage, to_stage, reason_code, reason_text, changed_by, source) VALUES ($1,$2,$3,$4,$5,$6,$7)',
        [oppId, fromStage, toStage, reason_code, reason_text, actor, 'user']
      );

      // Side effects
      if (toStage === 'AGREED') {
        // Customer (idempotent) if assignment demands
        if (normalizeAssignment(updatedOpp.assignment) === 'CUSTOMER') {
          const existing = await client.query('SELECT * FROM customers WHERE opportunity_id = $1', [oppId]);
          if (existing.rows.length === 0) {
            const customer_id = generateCustomerId();
            await client.query(
              'INSERT INTO customers (customer_id, opportunity_id, client_name, created_at) VALUES ($1,$2,$3,NOW())',
              [customer_id, oppId, client_name]
            );
            // Seed baseline audit for customer if not exists
            await client.query(
              'INSERT INTO customer_status_audit (customer_id, from_status, to_status, source) VALUES ($1,$2,$3,$4)',
              [customer_id, 'ACTIVE', 'ACTIVE', 'system']
            );
          } else {
            // Reactivate cancelled customers on reopen to AGREED
            for (const row of existing.rows) {
              if (row.customer_status && row.customer_status !== 'ACTIVE') {
                await client.query('UPDATE customers SET customer_status=$1 WHERE customer_id=$2', ['ACTIVE', row.customer_id]);
                await client.query('INSERT INTO customer_status_audit (customer_id, from_status, to_status, reason_code, reason_text, changed_by, source) VALUES ($1,$2,$3,$4,$5,$6,$7)', [row.customer_id, row.customer_status, 'ACTIVE', 'reopen', null, actor, 'user']);
              }
            }
          }
        }
        if (normalizeAssignment(updatedOpp.assignment) === 'CONTRACT') {
          const existingContract = await client.query('SELECT * FROM contracts WHERE opportunity_id = $1', [oppId]);
          if (existingContract.rows.length === 0) {
            const contract_id = Math.random().toString(36).substr(2, 6).toUpperCase();
            await client.query(
              `INSERT INTO contracts (
                contract_id, opportunity_id, client_name, quoted_price_per_litre, start_date, end_date, credit_period, primary_contact, phone_number, alt_phone, gstin, email, created_at
              ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW())`,
              [
                contract_id,
                updatedOpp.opportunity_id,
                client_name,
                proposed_price_per_litre,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
              ]
            );
            // Seed contract audit baseline
            await client.query('INSERT INTO contract_status_audit (contract_id, from_status, to_status, source) VALUES ($1,$2,$3,$4)', [contract_id, 'ACTIVE', 'ACTIVE', 'system']);
          } else {
            // Reactivate any cancelled contracts when reopening to AGREED
            for (const row of existingContract.rows) {
              if (row.contract_status === 'CANCELLED') {
                await client.query('UPDATE contracts SET contract_status=$1 WHERE contract_id=$2', ['ACTIVE', row.contract_id]);
                await client.query('INSERT INTO contract_status_audit (contract_id, from_status, to_status, reason_code, reason_text, changed_by, source) VALUES ($1,$2,$3,$4,$5,$6,$7)', [row.contract_id, 'CANCELLED', 'ACTIVE', 'reopen', null, actor, 'user']);
              }
            }
          }
        }
      }

  if (fromStage === 'AGREED' && toStage === 'CANCELLED') {
        // Cancel related contracts
        const contractsRes = await client.query('SELECT contract_id FROM contracts WHERE opportunity_id = $1', [oppId]);
        for (const row of contractsRes.rows) {
          await client.query('UPDATE contracts SET contract_status=$1 WHERE contract_id=$2', ['CANCELLED', row.contract_id]);
          await client.query(
            'INSERT INTO contract_status_audit (contract_id, from_status, to_status, reason_code, reason_text, changed_by, source) VALUES ($1,$2,$3,$4,$5,$6,$7)',
            [row.contract_id, 'ACTIVE', 'CANCELLED', reason_code || null, reason_text || null, actor, 'user']
          );
        }
        // Cancel related customers
        const customersRes = await client.query('SELECT customer_id, customer_status FROM customers WHERE opportunity_id = $1', [oppId]);
        for (const row of customersRes.rows) {
          if (row.customer_status !== 'CANCELLED') {
            await client.query('UPDATE customers SET customer_status=$1 WHERE customer_id=$2', ['CANCELLED', row.customer_id]);
            await client.query('INSERT INTO customer_status_audit (customer_id, from_status, to_status, reason_code, reason_text, changed_by, source) VALUES ($1,$2,$3,$4,$5,$6,$7)', [row.customer_id, row.customer_status || 'ACTIVE', 'CANCELLED', reason_code || null, reason_text || null, actor, 'user']);
          }
        }
      }
    }

    await client.query('COMMIT');
    res.json(updatedOpp);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error(err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});
app.delete('/api/opportunities/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM opportunities WHERE opportunity_id = $1 RETURNING *', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ error: 'Not found' });
    res.json({ success: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// =============================
// Opportunity Images Endpoints
// =============================
// Note: To avoid adding new dependencies, we'll accept base64 JSON uploads.
// POST /api/opportunities/:id/images
// Body: { fileName, mimeType, dataBase64 }
app.post('/api/opportunities/:id/images', requireAuth, async (req, res) => {
  try {
    await refreshFeatureFlags(pool);
    if (!featureFlags.hasImages) {
      return res.status(501).json({ error: 'Images feature not enabled on this server' });
    }
    const oppId = String(req.params.id || '').trim();
    if (!oppId) return res.status(400).json({ error: 'Invalid opportunity id' });
    // Validate opportunity exists
    const chk = await pool.query('SELECT 1 FROM opportunities WHERE opportunity_id=$1', [oppId]);
    if (!chk.rows.length) return res.status(404).json({ error: 'Opportunity not found' });
    let { fileName, mimeType, dataBase64 } = req.body || {};
    mimeType = (mimeType || '').toString().trim().toLowerCase();
    fileName = (fileName || '').toString().trim();
    const allowed = new Set(['image/png','image/jpeg','image/jpg','image/webp']);
    if (!allowed.has(mimeType)) return res.status(400).json({ error: 'Unsupported mimeType' });
    if (!dataBase64) return res.status(400).json({ error: 'dataBase64 is required' });
    // Accept with optional data URL prefix
    const comma = dataBase64.indexOf(',');
    if (comma !== -1) dataBase64 = dataBase64.slice(comma+1);
    let buf;
    try {
      buf = Buffer.from(dataBase64, 'base64');
    } catch (e) {
      return res.status(400).json({ error: 'Invalid base64 data' });
    }
    if (!buf || !buf.length) return res.status(400).json({ error: 'Empty file' });
    if (buf.length > 5 * 1024 * 1024) return res.status(400).json({ error: 'File too large (max 5MB)' });
    const actor = getActor(req);
    const actorId = req.user && req.user.sub;
    const ins = await pool.query(
      `INSERT INTO public.opportunity_images (opportunity_id, mime_type, file_name, file_size_bytes, data, created_by, created_by_user_id)
       VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id, opportunity_id, mime_type, file_name, file_size_bytes, created_at`,
      [oppId, mimeType, fileName || null, buf.length, buf, actor || null, actorId || null]
    );
    res.status(201).json(ins.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// GET list of images for an opportunity (metadata only)
app.get('/api/opportunities/:id/images', requireAuth, async (req, res) => {
  try {
    await refreshFeatureFlags(pool);
    if (!featureFlags.hasImages) {
      // Gracefully degrade when images table is not present
      return res.json([]);
    }
    const oppId = req.params.id;
    const r = await pool.query('SELECT id, opportunity_id, mime_type, file_name, file_size_bytes, created_at FROM public.opportunity_images WHERE opportunity_id=$1 ORDER BY created_at DESC', [oppId]);
    res.json(r.rows);
  } catch (err) { console.error(err); res.status(500).json({ error: err.message }); }
});

// GET stream image by id
app.get('/api/opportunities/:opportunityId/images/:imageId', async (req, res) => {
  try {
    await refreshFeatureFlags(pool);
    if (!featureFlags.hasImages) {
      return res.status(404).send('Not found');
    }
    const { opportunityId, imageId } = req.params;
    const r = await pool.query('SELECT mime_type, data FROM public.opportunity_images WHERE id=$1 AND opportunity_id=$2', [imageId, opportunityId]);
    if (!r.rows.length) return res.status(404).send('Not found');
    res.setHeader('Content-Type', r.rows[0].mime_type || 'application/octet-stream');
    res.setHeader('Cache-Control', 'private, max-age=300');
    return res.end(r.rows[0].data);
  } catch (err) { console.error(err); res.status(500).send('Server error'); }
});

// DELETE image
app.delete('/api/opportunities/:opportunityId/images/:imageId', requireAuth, async (req, res) => {
  try {
    await refreshFeatureFlags(pool);
    if (!featureFlags.hasImages) {
      return res.status(404).json({ error: 'Not found' });
    }
    const { opportunityId, imageId } = req.params;
    const del = await pool.query('DELETE FROM public.opportunity_images WHERE id=$1 AND opportunity_id=$2 RETURNING id', [imageId, opportunityId]);
    if (!del.rows.length) return res.status(404).json({ error: 'Not found' });
    res.json({ success: true });
  } catch (err) { console.error(err); res.status(500).json({ error: err.message }); }
});


// --- Status History CRUD ---
app.get('/api/status_history', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM status_history');
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});
app.post('/api/status_history', async (req, res) => {
  const { opportunity_id, stage, reason, at } = req.body;
  try {
    const result = await pool.query(
      'INSERT INTO status_history (opportunity_id, stage, reason, at) VALUES ($1,$2,$3,$4) RETURNING *',
      [opportunity_id, stage, reason, at]
    );
    res.status(201).json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// --- Expenses CRUD ---
// List expenses for an opportunity (ACTIVE only)
app.get('/api/opportunities/:id/expenses', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM expenses WHERE opportunity_id=$1 AND status=$2 ORDER BY at DESC, id DESC', [req.params.id, 'ACTIVE']);
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// Create expense for an opportunity with audit
app.post('/api/opportunities/:id/expenses', requireAuth, async (req, res) => {
  const oppId = req.params.id;
  let { amount, at, note } = req.body || {};
  // Normalize inputs
  amount = Number(amount);
  if (!Number.isFinite(amount) || amount <= 0) return res.status(400).json({ error: 'Amount must be a positive number' });
  // Accept yyyy-mm-dd or ISO datetime
  if (at) {
    const d = new Date(at);
    if (isNaN(d.getTime())) return res.status(400).json({ error: 'Invalid date' });
    // Convert to ISO without timezone ambiguity (store as timestamp without time zone)
    at = d.toISOString().slice(0,19).replace('T',' ');
  } else {
    at = null;
  }
  note = (note || '').toString().slice(0, 500);
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const ins = await client.query('INSERT INTO expenses (opportunity_id, amount, at, note, status, created_at, updated_at) VALUES ($1,$2,COALESCE($3,NOW()),$4,$5,NOW(),NOW()) RETURNING *', [oppId, amount, at, note, 'ACTIVE']);
    const row = ins.rows[0];
  const actor = getActor(req);
  await client.query('INSERT INTO expenses_audit (expense_id, opportunity_id, action, old_amount, new_amount, old_at, new_at, old_note, new_note, performed_by) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)', [row.id, oppId, 'CREATE', null, row.amount, null, row.at, null, row.note, actor]);
    await client.query('COMMIT');
    res.status(201).json(row);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error(err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// Update expense with audit
app.put('/api/expenses/:id', requireAuth, async (req, res) => {
  const id = req.params.id;
  let { amount, at, note } = req.body || {};
  if (amount !== undefined) {
    amount = Number(amount);
    if (!Number.isFinite(amount) || amount <= 0) return res.status(400).json({ error: 'Amount must be a positive number' });
  }
  if (at) {
    const d = new Date(at);
    if (isNaN(d.getTime())) return res.status(400).json({ error: 'Invalid date' });
    at = d.toISOString().slice(0,19).replace('T',' ');
  }
  note = note !== undefined ? (note || '').toString().slice(0,500) : undefined;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const curRes = await client.query('SELECT * FROM expenses WHERE id=$1', [id]);
    if (curRes.rows.length === 0) { await client.query('ROLLBACK'); return res.status(404).json({ error: 'Not found' }); }
    const cur = curRes.rows[0];
    const newAmount = amount !== undefined ? amount : cur.amount;
    const newAt = at !== undefined ? at : cur.at;
    const newNote = note !== undefined ? note : cur.note;
    const upd = await client.query('UPDATE expenses SET amount=$1, at=$2, note=$3, updated_at=NOW() WHERE id=$4 RETURNING *', [newAmount, newAt, newNote, id]);
    const row = upd.rows[0];
  const actor = getActor(req);
  await client.query('INSERT INTO expenses_audit (expense_id, opportunity_id, action, old_amount, new_amount, old_at, new_at, old_note, new_note, performed_by) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)', [row.id, row.opportunity_id, 'UPDATE', cur.amount, row.amount, cur.at, row.at, cur.note, row.note, actor]);
    await client.query('COMMIT');
    res.json(row);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error(err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// Delete expense (soft) with audit
app.delete('/api/expenses/:id', requireAuth, async (req, res) => {
  const id = req.params.id;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const curRes = await client.query('SELECT * FROM expenses WHERE id=$1', [id]);
    if (curRes.rows.length === 0) { await client.query('ROLLBACK'); return res.status(404).json({ error: 'Not found' }); }
    const cur = curRes.rows[0];
    await client.query('UPDATE expenses SET status=$1, updated_at=NOW() WHERE id=$2', ['DELETED', id]);
  const actor = getActor(req);
  await client.query('INSERT INTO expenses_audit (expense_id, opportunity_id, action, old_amount, new_amount, old_at, new_at, old_note, new_note, performed_by) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)', [cur.id, cur.opportunity_id, 'DELETE', cur.amount, null, cur.at, null, cur.note, null, actor]);
    await client.query('COMMIT');
    res.json({ success: true });
  } catch (err) {
    await client.query('ROLLBACK');
    console.error(err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// --- Meetings CRUD (enhanced) ---
// Normalize a timestamp from the client preserving LOCAL wall time.
// Accepts:
//  - "YYYY-MM-DD HH:mm[:ss]" (treated as local)
//  - "YYYY-MM-DDTHH:mm[:ss]" (treated as local)
//  - ISO with timezone (Z or +hh:mm)
// Always returns an SQL local timestamp string (YYYY-MM-DD HH:mm:ss)
function normalizeTimestamp(ts) {
  if (!ts) return null;
  // Reuse local-safe helpers defined below
  const d = toLocalDate(ts);
  if (!d) return null;
  return formatLocalSQL(d);
}

// --- Local time helpers (avoid UTC shifts for reminders) ---
function toLocalDate(ts) {
  if (!ts) return null;
  if (ts instanceof Date) return ts;
  const s = String(ts);
  // If string with timezone (Z or +hh:mm), let Date handle it
  if (/Z|[+-]\d{2}:\d{2}$/.test(s)) {
    const d = new Date(s);
    return isNaN(d.getTime()) ? null : d;
  }
  // Match YYYY-MM-DD HH:mm[:ss]
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?$/);
  if (m) {
    const [, Y, Mo, Da, H, Mi, S] = m;
    return new Date(+Y, +Mo - 1, +Da, +H, +Mi, +(S || 0), 0);
  }
  const d = new Date(s);
  return isNaN(d.getTime()) ? null : d;
}
function formatLocalSQL(d) {
  if (!(d instanceof Date)) return null;
  const pad = n => String(n).padStart(2,'0');
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}
function normalizeLocal(ts) {
  const d = toLocalDate(ts);
  if (!d) return null;
  return formatLocalSQL(d);
}

// Feature flags
const FEATURE_REMINDERS_AUDIT = /^(1|true|yes|on)$/i.test(String(process.env.FEATURE_REMINDERS_AUDIT || 'true'));

// --- Meetings audit helpers (v2: JSONB diff + snapshot) ---
function buildMeetingSnapshot(row) {
  if (!row) return null;
  return {
    id: row.id || null,
    customer_id: row.customer_id || null,
    opportunity_id: row.opportunity_id || null,
    contract_id: row.contract_id || null,
    subject: row.subject || null,
    starts_at: row.starts_at || row.when_ts || null,
    when_ts: row.when_ts || null,
    location: row.location || null,
    person_name: row.person_name || null,
    contact_phone: row.contact_phone || null,
    notes: row.notes || null,
    status: row.status || null,
    assigned_to: row.assigned_to || null,
    assigned_to_user_id: row.assigned_to_user_id || null,
    created_by: row.created_by || null,
    created_by_user_id: row.created_by_user_id || null,
    created_at: row.created_at || null,
    updated_at: row.updated_at || null,
    completed_at: row.completed_at || null
  };
}

// --- Reminders audit helpers (v2: JSONB diff + snapshot) ---
function buildReminderSnapshot(row) {
  if (!row) return null;
  return {
    id: row.id || null,
    title: row.title || null,
    due_ts: row.due_ts || row.when_ts || null,
    notify_at: row.notify_at || null,
    notes: row.notes || null,
    status: row.status || null,
    type: row.type || null,
    receiver_email: row.receiver_email || row.recipient_email || null,
    person_name: row.person_name || null,
    phone: row.phone || null,
    opportunity_id: row.opportunity_id || null,
    meeting_id: row.meeting_id || null,
    assigned_to: row.assigned_to || null,
    assigned_to_user_id: row.assigned_to_user_id || null,
    created_by: row.created_by || null,
    created_by_user_id: row.created_by_user_id || null,
    client_name: row.client_name || null,
    created_at: row.created_at || null,
    updated_at: row.updated_at || null
  };
}
function diffReminder(before, after) {
  if (!before || !after) return null;
  const keys = ['title','due_ts','notify_at','notes','status','type','receiver_email','person_name','phone','opportunity_id','meeting_id','assigned_to','assigned_to_user_id'];
  const d = {};
  for (const k of keys) {
    const bv = before[k] instanceof Date ? before[k].toISOString() : before[k];
    const av = after[k] instanceof Date ? after[k].toISOString() : after[k];
    const bvs = bv === undefined ? null : bv;
    const avs = av === undefined ? null : av;
    if (String(bvs) !== String(avs)) {
      d[k] = { from: bvs, to: avs };
    }
  }
  return Object.keys(d).length ? d : null;
}
async function insertReminderAuditV2(client, action, beforeRow, afterRow, note, req) {
  if (!FEATURE_REMINDERS_AUDIT) return;
  try {
    const reminderId = (afterRow && afterRow.id) || (beforeRow && beforeRow.id);
    if (!reminderId) return;
    const vr = await client.query('SELECT COALESCE(MAX(version),0)+1 AS v FROM reminders_audit_v2 WHERE reminder_id=$1', [String(reminderId)]);
    const version = (vr.rows[0] && vr.rows[0].v) ? Number(vr.rows[0].v) : 1;
    const actorUserId = req.user && req.user.sub ? req.user.sub : null;
    const actor = getActor(req);
    const diff = beforeRow && afterRow ? diffReminder(buildReminderSnapshot(beforeRow), buildReminderSnapshot(afterRow)) : null;
    const snapshotObj = afterRow ? buildReminderSnapshot(afterRow) : (beforeRow ? buildReminderSnapshot(beforeRow) : null);
    const diffStr = diff ? JSON.stringify(diff) : null;
    const snapStr = snapshotObj ? JSON.stringify(snapshotObj) : null;
    const reminderType = (afterRow && afterRow.type) || (beforeRow && beforeRow.type) || (snapshotObj && snapshotObj.type) || null;
    await client.query(
      `INSERT INTO reminders_audit_v2 (reminder_id, version, action, performed_by_user_id, performed_by, diff, snapshot, note, reminder_type)
       VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8,$9)`,
      [String(reminderId), version, action, actorUserId, actor || null, diffStr, snapStr, note || null, reminderType]
    );
  } catch (e) {
    console.warn('[reminders_audit_v2] insert failed:', e.message);
  }
}
function diffMeeting(before, after) {
  if (!before || !after) return null;
  const keys = ['customer_id','opportunity_id','contract_id','subject','starts_at','when_ts','location','person_name','contact_phone','notes','status','assigned_to','assigned_to_user_id'];
  const d = {};
  for (const k of keys) {
    const bv = before[k] instanceof Date ? before[k].toISOString() : before[k];
    const av = after[k] instanceof Date ? after[k].toISOString() : after[k];
    const bvs = bv === undefined ? null : bv;
    const avs = av === undefined ? null : av;
    if (String(bvs) !== String(avs)) {
      d[k] = { from: bvs, to: avs };
    }
  }
  return Object.keys(d).length ? d : null;
}
async function insertMeetingAuditV2(client, action, beforeRow, afterRow, note, req) {
  try {
    const meetingId = (afterRow && afterRow.id) || (beforeRow && beforeRow.id);
    if (!meetingId) return;
    const vr = await client.query('SELECT COALESCE(MAX(version),0)+1 AS v FROM meetings_audit_v2 WHERE meeting_id=$1', [meetingId]);
    const version = (vr.rows[0] && vr.rows[0].v) ? Number(vr.rows[0].v) : 1;
    const actorUserId = req.user && req.user.sub ? req.user.sub : null;
    const actor = getActor(req);
    const diff = beforeRow && afterRow ? diffMeeting(beforeRow, afterRow) : null;
    const snapshotObj = afterRow ? buildMeetingSnapshot(afterRow) : (beforeRow ? buildMeetingSnapshot(beforeRow) : null);
    const diffStr = diff ? JSON.stringify(diff) : null;
    const snapStr = snapshotObj ? JSON.stringify(snapshotObj) : null;
    await client.query(
      `INSERT INTO meetings_audit_v2 (meeting_id, version, action, performed_by_user_id, performed_by, diff, snapshot, note)
       VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8)`,
      [meetingId, version, action, actorUserId, actor || null, diffStr, snapStr, note || null]
    );
  } catch (e) {
    // best-effort; don't block main flow on audit failure
    console.warn('[meetings_audit_v2] insert failed:', e.message);
  }
}

// Helper: build a meeting DTO for email/template/ICS
function buildMeetingDto({ id, subject, clientName, personName, startsAt, endsAt, location, meetingLink }) {
  const start = new Date(startsAt);
  const end = endsAt ? new Date(endsAt) : new Date(start.getTime() + 60 * 60 * 1000);
  const dOpts = { weekday: 'short', year: 'numeric', month: 'short', day: 'numeric' };
  const tOpts = { hour: 'numeric', minute: '2-digit' };
  const dateText = start.toLocaleDateString('en-IN', dOpts);
  const timeText = `${start.toLocaleTimeString('en-IN', tOpts)} – ${end.toLocaleTimeString('en-IN', tOpts)}`;
  return {
    id,
    title: subject,
    clientName,
    personName: personName || null,
    startsAt: start,
    endsAt: end,
    location: location || null,
    meetingLink: meetingLink || null,
    dateText,
    timeText,
  };
}

// List meetings with filters & pagination (scoped)
app.get('/api/meetings', requireAuth, async (req, res) => {
  let { q, dateFrom, dateTo, status, customerId, opportunityId, contractId, assignedTo, assignedToUserId, userId, createdBy, page = 1, pageSize = 50, sort } = req.query;
  // Validate/clamp pagination
  const p = clampInt(page, 1, 100000, 1);
  const s = clampInt(pageSize, 1, 500, 50);
  const offset = (p - 1) * s;
  const params = [];
  const filters = [];
  // Validate status allowlist
  if (status) {
    const allowed = ['SCHEDULED','COMPLETED','CANCELLED','NO_SHOW','RESCHEDULED'];
    const list = safeList(String(status).split(',').map(x => x.trim()).filter(Boolean), allowed);
    status = list.join(',');
  }
  // Validate date range
  if (dateFrom && !isValidDateTimeString(dateFrom)) dateFrom = null;
  if (dateTo && !isValidDateTimeString(dateTo)) dateTo = null;
  // Validate explicit user-scoping ids for OWNER/ADMIN
  let validAssignedToUserId = null;
  let validUserId = null;
  try {
    // Allow OWNER/ADMIN to query any selectable user; allow EMPLOYEE only when assignedToUserId === self
    if (assignedToUserId) {
      if (req.user.role === 'OWNER' || req.user.role === 'ADMIN') {
        validAssignedToUserId = await validateSelectableUserId(pool, String(assignedToUserId));
        if (!validAssignedToUserId) return res.status(400).json({ error: 'Invalid assignedToUserId' });
      } else if (req.user.role === 'EMPLOYEE') {
        const createdBySelf = createdBy && String(createdBy).toLowerCase() === 'self';
        if (createdBySelf) {
          // In 'Assigned To' tab, employee can view items they created assigned to others
          validAssignedToUserId = await validateSelectableUserId(pool, String(assignedToUserId));
          if (!validAssignedToUserId) return res.status(400).json({ error: 'Invalid assignedToUserId' });
        } else {
          // Otherwise restrict to self only
          if (String(assignedToUserId) === String(req.user.sub)) {
            validAssignedToUserId = req.user.sub;
          } else {
            return res.status(403).json({ error: 'Forbidden assignedToUserId' });
          }
        }
      }
    }
    if (userId && (req.user.role === 'OWNER' || req.user.role === 'ADMIN')) {
      // Allow Admin/Owner querying themselves even if ADMIN is not selectable
      if (String(userId) === String(req.user.sub)) {
        validUserId = req.user.sub;
      } else {
        validUserId = await validateSelectableUserId(pool, String(userId));
        if (!validUserId) return res.status(400).json({ error: 'Invalid userId' });
      }
    }
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
  if (customerId) { params.push(customerId); filters.push(`m.customer_id = $${params.length}`); }
  if (opportunityId) { params.push(opportunityId); filters.push(`m.opportunity_id = $${params.length}`); }
  if (contractId) { params.push(contractId); filters.push(`m.contract_id = $${params.length}`); }
  if (validAssignedToUserId) {
    // When filtering by assigned user, also include legacy string matches on assigned_to for robustness
    params.push(validAssignedToUserId);
    const base = params.length;
    const legacyIds = [];
    if (validAssignedToUserId) {
      const ur = await pool.query('SELECT email, username, full_name FROM public.users WHERE id=$1', [validAssignedToUserId]);
      if (ur.rows.length) {
        const { email, username, full_name } = ur.rows[0];
        [email, username, full_name].forEach(v => { if (v && String(v).trim().length) legacyIds.push(v); });
      }
    }
    let legacyClause = '';
    if (legacyIds.length) {
      const ph = legacyIds.map((_, i) => `$${base + i + 1}`).join(',');
      params.push(...legacyIds);
      legacyClause = ` OR m.assigned_to IN (${ph})`;
    }
    filters.push(`(m.assigned_to_user_id = $${base}${legacyClause})`);
  }
  // Owner/Admin explicit per-user filter: include meetings where selected user is assignee or creator
  if (validUserId && req.user && (req.user.role === 'OWNER' || req.user.role === 'ADMIN')) {
    params.push(validUserId, validUserId);
    filters.push(`(m.assigned_to_user_id = $${params.length-1} OR m.created_by_user_id = $${params.length})`);
  }
  if (assignedTo) { params.push(assignedTo); filters.push(`m.assigned_to = $${params.length}`); }
  // createdBy=self filter (intersection with other filters)
  if (createdBy && String(createdBy).toLowerCase() === 'self') {
    const selfId = req.user && req.user.sub;
    const actor = getActor(req);
    if (selfId) {
      params.push(selfId);
      const base = params.length;
      filters.push(`(m.created_by_user_id = $${base}`);
      // legacy fallbacks on created_by (text)
      const ids = [req.user.email, req.user.username, req.user.full_name, actor].filter(v => v && String(v).trim().length);
      if (ids.length) {
        const ph = ids.map((_, i) => `$${base + i + 1}`).join(',');
        params.push(...ids);
        filters[filters.length - 1] += ` OR m.created_by IN (${ph})`;
      }
      filters[filters.length - 1] += `)`;
    } else {
      // fallback to actor string only
      const act = actor || '';
      if (act) { params.push(act); filters.push(`m.created_by = $${params.length}`); }
    }
  }
  // Role-based visibility: EMPLOYEE can only see meetings assigned to them or created by them
  if (req.user && req.user.role === 'EMPLOYEE') {
    const userId = req.user.sub;
    // Prefer fast/equality checks on user_id columns with fallback to legacy text
    const ids = [req.user.email, req.user.username, req.user.full_name].filter(x => x && String(x).trim().length);
    const conds = [];
    if (userId) {
      params.push(userId, userId);
      conds.push(`m.assigned_to_user_id = $${params.length-1}`);
      conds.push(`m.created_by_user_id = $${params.length}`);
    }
    if (ids.length) {
      const base = params.length;
      params.push(...ids);
      const placeholders = ids.map((_, i) => `$${base + i + 1}`).join(',');
      const base2 = params.length;
      params.push(...ids);
      const placeholders2 = ids.map((_, i) => `$${base2 + i + 1}`).join(',');
      conds.push(`m.assigned_to IN (${placeholders})`);
      conds.push(`m.created_by IN (${placeholders2})`);
    } else {
      const actor = getActor(req);
      params.push(actor, actor);
      conds.push(`m.assigned_to = $${params.length-1}`);
      conds.push(`m.created_by = $${params.length}`);
    }
    filters.push(`(${conds.join(' OR ')})`);
  }
  if (status) {
    const list = String(status).split(',').map(x => x.trim()).filter(Boolean);
    if (list.length) {
      const ph = list.map((_, i) => `$${params.length + i + 1}`).join(',');
      params.push(...list);
      filters.push(`m.status IN (${ph})`);
    }
  }
  if (dateFrom) { params.push(dateFrom); filters.push(`m.starts_at >= $${params.length}`); }
  if (dateTo) { params.push(dateTo); filters.push(`m.starts_at <= $${params.length}`); }
  if (q) {
    params.push(`%${String(q).toLowerCase()}%`);
    const idx = params.length;
    filters.push(`(
      LOWER(m.subject) LIKE $${idx}
      OR LOWER(m.location) LIKE $${idx}
      OR LOWER(m.id) LIKE $${idx}
      OR LOWER(m.customer_id) LIKE $${idx}
      OR LOWER(COALESCE(m.opportunity_id,'')) LIKE $${idx}
      OR LOWER(COALESCE(m.contract_id,'')) LIKE $${idx}
      OR LOWER(COALESCE(c.client_name,'')) LIKE $${idx}
    )`);
  }
  const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';
  const order = (String(sort||'').toLowerCase() === 'starts_at_asc') ? 'ORDER BY m.starts_at ASC NULLS LAST' : 'ORDER BY m.starts_at DESC NULLS LAST';
  try {
    const sql = `
      SELECT m.*,
             COALESCE(c.client_name, o.client_name) AS client_name,
             ua.full_name AS assigned_to_full_name,
             ua.username AS assigned_to_username,
             ua.email AS assigned_to_email,
             uc.full_name AS created_by_full_name,
             uc.username AS created_by_username,
             uc.email AS created_by_email,
             COALESCE(mea.emails_sent_count, 0) AS emails_sent_count
      FROM meetings m
      LEFT JOIN customers c ON c.customer_id = m.customer_id
      LEFT JOIN opportunities o ON o.opportunity_id = m.opportunity_id
      LEFT JOIN public.users ua ON ua.id = m.assigned_to_user_id
      LEFT JOIN public.users uc ON uc.id = m.created_by_user_id
      LEFT JOIN (
        SELECT meeting_id, (COUNT(*) FILTER (WHERE status = 'SENT'))::int AS emails_sent_count
          FROM meeting_email_audit
         GROUP BY meeting_id
      ) mea ON mea.meeting_id = m.id
      ${where}
      ${order}
      LIMIT ${s} OFFSET ${offset}
    `;
    const rows = await pool.query(sql, params);
    res.json({ items: rows.rows, page: p, pageSize: s });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// Generate ICS for a specific meeting (public: used from email buttons)
app.get('/api/meetings/:id/ics', async (req, res) => {
  try {
    const id = req.params.id;
    const r = await pool.query(`
      SELECT m.id, m.subject, m.starts_at, m.location, m.person_name, m.meeting_link,
             COALESCE(c.client_name, o.client_name) AS client_name
        FROM meetings m
        LEFT JOIN customers c ON c.customer_id = m.customer_id
        LEFT JOIN opportunities o ON o.opportunity_id = m.opportunity_id
       WHERE m.id = $1
       LIMIT 1
    `, [id]);
    if (!r.rows.length) return res.status(404).send('Not found');
    const row = r.rows[0];
    const dto = buildMeetingDto({ id: row.id, subject: row.subject, clientName: row.client_name, personName: row.person_name, startsAt: row.starts_at, endsAt: null, location: row.location, meetingLink: row.meeting_link });
    const ics = await generateICS(dto);
    res.setHeader('Content-Type', 'text/calendar; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="meeting-${encodeURIComponent(id)}.ics"`);
    return res.send(ics);
  } catch (e) {
    console.error(e);
    res.status(500).send('Server error');
  }
});

// Generate a single ICS file containing multiple reminder events (public: used from email buttons)
// Query: /api/reminders/ics?ids=ID1,ID2,ID3
app.get('/api/reminders/ics', async (req, res) => {
  try {
    const idsParam = String(req.query.ids || '').trim();
    if (!idsParam) return res.status(400).send('ids query is required');
    const ids = idsParam.split(',').map(s => s.trim()).filter(Boolean).slice(0, 200);
    if (!ids.length) return res.status(400).send('No valid ids');
    const ph = ids.map((_, i) => `$${i + 1}`).join(',');
    const r = await pool.query(
      `SELECT id, title, type, notes, due_ts AS when, person_name, phone, client_name
         FROM reminders
        WHERE id IN (${ph})
        ORDER BY due_ts ASC NULLS LAST`, ids);
    if (!r.rows.length) return res.status(404).send('Not found');
    const ics = await generateICSMultiForReminders(r.rows.map(row => ({
      id: row.id,
      title: row.title,
      type: row.type,
      notes: row.notes,
      when: row.when,
      person_name: row.person_name,
      phone: row.phone,
      client_name: row.client_name
    })));
    res.setHeader('Content-Type', 'text/calendar; charset=utf-8');
    res.setHeader('X-Content-Type-Options', 'nosniff');
    const today = new Date();
    const ymd = today.toISOString().slice(0,10).replace(/-/g,'');
    res.setHeader('Content-Disposition', `attachment; filename="reminders-${ymd}.ics"`);
    return res.send(ics);
  } catch (e) {
    console.error(e);
    res.status(500).send('Server error');
  }
});

// Create meeting with audit
app.post('/api/meetings', requireAuth, async (req, res) => {
  let { id, customer_id, opportunity_id, contract_id, subject, starts_at, when_ts, location, person_name, contact_phone, notes, status, assigned_to, assignedToUserId, meeting_link, meetingLink } = req.body || {};
  subject = (subject || '').toString().trim();
  const start = normalizeTimestamp(starts_at || when_ts);
  // Allow meetings for leads: either customer_id OR opportunity_id must be present
  if (!customer_id && !opportunity_id) {
    return res.status(400).json({ error: 'Either opportunity_id or customer_id is required' });
  }
  if (!subject) return res.status(400).json({ error: 'subject is required' });
  // person_name required at UI level; enforce softly (can be empty in legacy)
  person_name = person_name !== undefined ? (person_name || null) : null;
  contact_phone = contact_phone !== undefined ? (contact_phone || null) : null;
  if (!start) return res.status(400).json({ error: 'starts_at is invalid or missing' });
  if (!id) id = Math.random().toString(36).slice(2, 10).toUpperCase();
  const st = (status || 'SCHEDULED').toUpperCase();
  const actor = getActor(req);
  const actorUserId = req.user && req.user.sub;
  // Enforce creator and assignment rules
  const role = (req.user && req.user.role) || 'EMPLOYEE';
  let assignedUserId = null;
  let assignedLabel = null;
  if (role === 'EMPLOYEE') {
    assignedUserId = actorUserId; // employees can only assign to themselves
    // get a stable label from users table
    const r = await pool.query('SELECT email, username, full_name FROM public.users WHERE id=$1', [actorUserId]);
    assignedLabel = r.rows.length ? pickDisplay(r.rows[0]) : actor;
  } else {
    if (assignedToUserId) {
      // Prefer explicit user id provided by client
      assignedUserId = assignedToUserId;
      const rr = await pool.query('SELECT email, username, full_name FROM public.users WHERE id=$1 AND active=TRUE', [assignedToUserId]);
      assignedLabel = rr.rows.length ? pickDisplay(rr.rows[0]) : null;
    }
    if (!assignedUserId) {
      // Fall back to legacy string field
      const u = assigned_to ? await resolveUserByIdentifier(assigned_to) : null;
      if (u) {
        assignedUserId = u.id;
        assignedLabel = pickDisplay(u);
      }
    }
    if (!assignedUserId) {
      // final fallback to actor (owner/admin)
      assignedUserId = actorUserId;
      const r = await pool.query('SELECT email, username, full_name FROM public.users WHERE id=$1', [actorUserId]);
      assignedLabel = r.rows.length ? pickDisplay(r.rows[0]) : actor;
    }
  }
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // Derive opportunity_id from customer if not provided, and validate FK when present
    if (!opportunity_id && customer_id) {
      const r = await client.query('SELECT opportunity_id FROM customers WHERE customer_id = $1', [customer_id]);
      if (r.rows.length) opportunity_id = r.rows[0].opportunity_id;
    }
    if (opportunity_id) {
      const chk = await client.query('SELECT 1 FROM opportunities WHERE opportunity_id = $1', [opportunity_id]);
      if (chk.rows.length === 0) {
        await client.query('ROLLBACK');
        return res.status(400).json({ error: 'Invalid opportunity_id (no such opportunity)' });
      }
    }
    // At this point, opportunity_id may be null only if customer_id was provided but could not be derived.
    // That would indicate data inconsistency; prevent creation in that case.
    if (!opportunity_id) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'Could not resolve opportunity_id for provided customer_id' });
    }
    // Validate contract_id belongs to the opportunity if provided; otherwise clear it
    if (contract_id) {
      const ck = await client.query('SELECT 1 FROM contracts WHERE contract_id = $1 AND opportunity_id = $2', [contract_id, opportunity_id]);
      if (ck.rows.length === 0) {
        contract_id = null; // don't allow FK mismatch
      }
    }
    const ins = await client.query(
      `INSERT INTO meetings (
         id, customer_id, opportunity_id, contract_id, subject, starts_at, when_ts, location, person_name, contact_phone, notes, status,
         assigned_to, assigned_to_user_id, meeting_link, created_by, created_by_user_id, created_at, updated_at)
       VALUES (
         $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
         $13,$14,$15,$16,$17,NOW(),NOW()
       ) RETURNING *`,
      [id, customer_id, opportunity_id || null, contract_id || null, subject, start, start, location || null, person_name, contact_phone, notes || null, st,
       assignedLabel || null, assignedUserId || null, (meeting_link || meetingLink || null), actor, actorUserId]
    );
    const row = ins.rows[0];
    await client.query(
      `INSERT INTO meetings_audit (meeting_id, action, performed_by, before_subject, after_subject, before_starts_at, after_starts_at, before_status, after_status, outcome_notes)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
      [row.id, 'CREATE', actor, null, row.subject, null, row.starts_at, null, row.status, null]
    );
    await insertMeetingAuditV2(client, 'CREATE', null, row, null, req);
    await client.query('COMMIT');
    res.status(201).json(row);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error(err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// Update meeting with audit
app.put('/api/meetings/:id', requireAuth, async (req, res) => {
  const id = req.params.id;
  let { customer_id, opportunity_id, contract_id, subject, starts_at, when_ts, location, person_name, contact_phone, notes, status, assigned_to, assignedToUserId, meeting_link, meetingLink } = req.body || {};
  const start = starts_at || when_ts ? normalizeTimestamp(starts_at || when_ts) : undefined;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const curRes = await client.query('SELECT * FROM meetings WHERE id=$1', [id]);
    if (curRes.rows.length === 0) { await client.query('ROLLBACK'); return res.status(404).json({ error: 'Not found' }); }
    const cur = curRes.rows[0];
    // Guard: Employees may edit only meetings they created
    if (req.user && req.user.role === 'EMPLOYEE') {
      const actor = getActor(req);
      if (String(cur.created_by || '') !== String(actor)) {
        await client.query('ROLLBACK');
        return res.status(403).json({ error: 'Employees can edit only meetings they created' });
      }
    }
    const newSubject = subject !== undefined ? String(subject || '').trim() : cur.subject;
    const newStartsAt = start !== undefined ? start : cur.starts_at;
    const newStatus = status !== undefined ? String(status || '').toUpperCase() : cur.status;
    const newNotes = notes !== undefined ? notes : cur.notes;
    let newOpportunityId = opportunity_id || cur.opportunity_id;
    if (!newOpportunityId && (customer_id || cur.customer_id)) {
      const r = await client.query('SELECT opportunity_id FROM customers WHERE customer_id = $1', [customer_id || cur.customer_id]);
      if (r.rows.length) newOpportunityId = r.rows[0].opportunity_id;
    }
    if (newOpportunityId) {
      const chk = await client.query('SELECT 1 FROM opportunities WHERE opportunity_id = $1', [newOpportunityId]);
      if (chk.rows.length === 0) { await client.query('ROLLBACK'); return res.status(400).json({ error: 'Invalid opportunity_id (no such opportunity)' }); }
    }
    let newContractId = contract_id !== undefined ? contract_id : cur.contract_id;
    if (newContractId) {
      const ck = await client.query('SELECT 1 FROM contracts WHERE contract_id = $1 AND opportunity_id = $2', [newContractId, newOpportunityId]);
      if (ck.rows.length === 0) newContractId = null;
    }
    // Normalize assignee: prefer user_id when provided; employees cannot reassign off themselves
    let newAssignedUserId = cur.assigned_to_user_id || null;
    let assignedNorm = assigned_to !== undefined ? assigned_to : cur.assigned_to;
    if (req.user && req.user.role === 'EMPLOYEE') {
      // lock to current actor
      newAssignedUserId = req.user.sub;
      const rlab = await pool.query('SELECT email, username, full_name FROM public.users WHERE id=$1', [newAssignedUserId]);
      assignedNorm = rlab.rows.length ? pickDisplay(rlab.rows[0]) : getActor(req);
    } else {
      if (assignedToUserId !== undefined && assignedToUserId) {
        newAssignedUserId = assignedToUserId;
        const rlab = await pool.query('SELECT email, username, full_name FROM public.users WHERE id=$1 AND active=TRUE', [assignedToUserId]);
        assignedNorm = rlab.rows.length ? pickDisplay(rlab.rows[0]) : assignedNorm;
      } else if (assigned_to !== undefined) {
        const u = await resolveUserByIdentifier(assigned_to);
        if (u) {
          newAssignedUserId = u.id;
          assignedNorm = pickDisplay(u);
        } else if (assigned_to === null) {
          // Explicitly clear (rare)
          newAssignedUserId = null;
          assignedNorm = null;
        }
      }
    }
    const upd = await client.query(
      `UPDATE meetings SET customer_id=$1, opportunity_id=$2, contract_id=$3, subject=$4, starts_at=$5, when_ts=$6, location=$7, person_name=$8, contact_phone=$9, notes=$10, status=$11, assigned_to=$12, assigned_to_user_id=$13, meeting_link=$14, updated_at=NOW()
       WHERE id=$15 RETURNING *`,
      [
        customer_id || cur.customer_id,
        newOpportunityId || cur.opportunity_id,
        newContractId || null,
        newSubject,
        newStartsAt,
        newStartsAt,
        location !== undefined ? location : cur.location,
        person_name !== undefined ? (person_name || null) : cur.person_name,
        contact_phone !== undefined ? (contact_phone || null) : cur.contact_phone,
        newNotes,
        newStatus,
        assignedNorm,
        newAssignedUserId,
        (meeting_link !== undefined ? (meeting_link || meetingLink || null) : cur.meeting_link),
        id
      ]
    );
    const row = upd.rows[0];
    const outcomeNotes = req.body.outcomeNotes || req.body.outcome_notes || null;
    const actor = getActor(req);
    await client.query(
      `INSERT INTO meetings_audit (meeting_id, action, performed_by,
         before_subject, after_subject, before_starts_at, after_starts_at, before_status, after_status, outcome_notes)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
      [row.id, 'UPDATE', actor, cur.subject, row.subject, cur.starts_at, row.starts_at, cur.status, row.status, outcomeNotes]
    );
    await insertMeetingAuditV2(client, 'UPDATE', cur, row, outcomeNotes, req);
    await client.query('COMMIT');
    res.json(row);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error(err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// Complete a meeting
app.patch('/api/meetings/:id/complete', requireAuth, async (req, res) => {
  const id = req.params.id;
  const { performed_by, outcome, note } = req.body || {};
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const curRes = await client.query('SELECT * FROM meetings WHERE id=$1', [id]);
    if (curRes.rows.length === 0) { await client.query('ROLLBACK'); return res.status(404).json({ error: 'Not found' }); }
    const cur = curRes.rows[0];
    // Guard for EMPLOYEE
    if (req.user && req.user.role === 'EMPLOYEE') {
      const actor = getActor(req);
      if (cur.created_by_user_id !== req.user.sub && String(cur.created_by || '') !== String(actor)) {
        await client.query('ROLLBACK');
        return res.status(403).json({ error: 'Employees can update only meetings they created' });
      }
    }
    const outcomeText = (outcome || note || '').toString().trim();
    const newNotes = outcomeText ? `${cur.notes ? cur.notes + '\n' : ''}Outcome: ${outcomeText}` : cur.notes;
    const upd = await client.query('UPDATE meetings SET status=$1, notes=$2, completed_at=NOW(), updated_at=NOW() WHERE id=$3 RETURNING *', ['COMPLETED', newNotes, id]);
    const row = upd.rows[0];
    const actor = getActor(req);
    await client.query(
      'INSERT INTO meetings_audit (meeting_id, action, performed_by, before_status, after_status, outcome_notes) VALUES ($1,$2,$3,$4,$5,$6)',
      [row.id, 'COMPLETE', performed_by || actor, cur.status, row.status, outcomeText || null]
    );
    await insertMeetingAuditV2(client, 'COMPLETE', cur, row, outcomeText || null, req);
    await client.query('COMMIT');
    res.json(row);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error(err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// Cancel a meeting
app.patch('/api/meetings/:id/cancel', requireAuth, async (req, res) => {
  const id = req.params.id;
  const { reason, performed_by } = req.body || {};
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const curRes = await client.query('SELECT * FROM meetings WHERE id=$1', [id]);
    if (curRes.rows.length === 0) { await client.query('ROLLBACK'); return res.status(404).json({ error: 'Not found' }); }
    const cur = curRes.rows[0];
    // Guard for EMPLOYEE
    if (req.user && req.user.role === 'EMPLOYEE') {
      const actor = getActor(req);
      if (cur.created_by_user_id !== req.user.sub && String(cur.created_by || '') !== String(actor)) {
        await client.query('ROLLBACK');
        return res.status(403).json({ error: 'Employees can update only meetings they created' });
      }
    }
    const newNotes = reason ? `${cur.notes ? cur.notes + '\n' : ''}Cancelled: ${reason}` : cur.notes;
    const upd = await client.query('UPDATE meetings SET status=$1, notes=$2, updated_at=NOW() WHERE id=$3 RETURNING *', ['CANCELLED', newNotes, id]);
    const row = upd.rows[0];
    const actor = getActor(req);
    await client.query(
      'INSERT INTO meetings_audit (meeting_id, action, performed_by, before_status, after_status, outcome_notes) VALUES ($1,$2,$3,$4,$5,$6)',
      [row.id, 'CANCEL', performed_by || actor, cur.status, row.status, reason || null]
    );
    await insertMeetingAuditV2(client, 'CANCEL', cur, row, reason || null, req);
    await client.query('COMMIT');
    res.json(row);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error(err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// Delete meeting (hard delete) with audit record
app.delete('/api/meetings/:id', requireAuth, async (req, res) => {
  const id = req.params.id;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const curRes = await client.query('SELECT * FROM meetings WHERE id=$1', [id]);
    if (curRes.rows.length === 0) { await client.query('ROLLBACK'); return res.status(404).json({ error: 'Not found' }); }
    const cur = curRes.rows[0];
    // Guard for EMPLOYEE
    if (req.user && req.user.role === 'EMPLOYEE') {
      const actor = getActor(req);
      if (cur.created_by_user_id !== req.user.sub && String(cur.created_by || '') !== String(actor)) {
        await client.query('ROLLBACK');
        return res.status(403).json({ error: 'Employees can delete only meetings they created' });
      }
    }
    await client.query('DELETE FROM meetings WHERE id=$1', [id]);
  const actor = getActor(req);
  await client.query('INSERT INTO meetings_audit (meeting_id, action, performed_by, before_subject, before_starts_at, before_status, outcome_notes) VALUES ($1,$2,$3,$4,$5,$6,$7)', [cur.id, 'DELETE', actor, cur.subject, cur.starts_at, cur.status, null]);
  await insertMeetingAuditV2(client, 'DELETE', cur, null, null, req);
    await client.query('COMMIT');
    res.json({ success: true });
  } catch (err) {
    await client.query('ROLLBACK');
    console.error(err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// Meetings Audit list
app.get('/api/meetings-audit', async (req, res) => {
  const { meetingId, action, dateFrom, dateTo, page = 1, pageSize = 50 } = req.query;
  const p = Math.max(Number(page) || 1, 1);
  const s = Math.min(Math.max(Number(pageSize) || 50, 1), 500);
  const offset = (p - 1) * s;
  const params = [];
  const filters = [];
  if (meetingId) { params.push(meetingId); filters.push(`ma.meeting_id = $${params.length}`); }
  if (action) {
    const actions = String(action).split(',').map(x => x.trim().toUpperCase()).filter(Boolean);
    if (actions.length) {
      const ph = actions.map((_, i) => `$${params.length + i + 1}`).join(',');
      params.push(...actions);
      filters.push(`ma.action IN (${ph})`);
    }
  }
  if (dateFrom) { params.push(dateFrom); filters.push(`ma.performed_at >= $${params.length}`); }
  if (dateTo) { params.push(dateTo); filters.push(`ma.performed_at <= $${params.length}`); }
  const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';
  try {
    const sql = `
      SELECT ma.*, m.customer_id, m.opportunity_id, m.contract_id
      FROM meetings_audit ma
      LEFT JOIN meetings m ON m.id = ma.meeting_id
      ${where}
      ORDER BY ma.performed_at DESC
      LIMIT ${s} OFFSET ${offset}
    `;
    const rows = await pool.query(sql, params);
    res.json({ items: rows.rows, page: p, pageSize: s });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// --- Reminders CRUD ---
app.get('/api/reminders', requireAuth, async (req, res) => {
  try {
    let { type, meetingId, opportunityId, dateFrom, dateTo, status, userId, assignedToUserId, createdBy, page = 1, pageSize = 100, sort } = req.query;
    const p = clampInt(page, 1, 100000, 1);
    const s = clampInt(pageSize, 1, 500, 100);
    const offset = (p - 1) * s;
    const params = [];
    const filters = [];
    // Validate allowlists
    if (type) {
      const allowedTypes = ['CALL','EMAIL','MEETING'];
      const types = safeList(String(type).split(',').map(x => x.trim()), allowedTypes);
      if (types.length) {
        const ph = types.map((_, i) => `$${params.length + i + 1}`).join(',');
        params.push(...types);
        filters.push(`r.type IN (${ph})`);
      }
    }
    if (meetingId) { params.push(meetingId); filters.push(`r.meeting_id = $${params.length}`); }
    if (opportunityId) { params.push(opportunityId); filters.push(`r.opportunity_id = $${params.length}`); }
    if (status) {
      const allowedStatuses = ['PENDING','DONE','SENT','FAILED'];
      const statuses = safeList(String(status).split(',').map(x => x.trim()), allowedStatuses);
      if (statuses.length) {
        const ph = statuses.map((_, i) => `$${params.length + i + 1}`).join(',');
        params.push(...statuses);
        filters.push(`r.status IN (${ph})`);
      }
    }
    if (dateFrom && isValidDateTimeString(dateFrom)) { params.push(dateFrom); filters.push(`r.due_ts >= $${params.length}`); }
    if (dateTo && isValidDateTimeString(dateTo)) { params.push(dateTo); filters.push(`r.due_ts <= $${params.length}`); }
  // Owner/Admin explicit per-user filter, mirrors employee-overview logic
    if (userId && req.user && (req.user.role === 'OWNER' || req.user.role === 'ADMIN')) {
      const self = String(userId) === String(req.user.sub) ? req.user.sub : null;
      const valid = self ? self : await validateSelectableUserId(pool, String(userId));
      if (!valid) return res.status(400).json({ error: 'Invalid userId' });
      const u = valid;
      // Also fetch identifier strings for legacy rows
      const ur = await pool.query('SELECT email, username, full_name FROM public.users WHERE id=$1', [u]);
      const parts = ur.rows.length ? [ur.rows[0].email, ur.rows[0].username, ur.rows[0].full_name].filter(v => v && String(v).trim().length) : [];
      // param order: [created_by_user_id, assigned_to_user_id, m.assigned_to_user_id, m.created_by_user_id, ...identifiers]
      params.push(u, u, u, u);
      const base = params.length;
      let legacy = '';
      if (parts.length) {
        const placeholders = parts.map((_, i) => `$${base + i + 1}`).join(',');
        params.push(...parts);
        legacy = ` OR r.created_by IN (${placeholders}) OR r.assigned_to IN (${placeholders})`;
      }
      filters.push(`(
        r.created_by_user_id = $${base-3}
        OR r.assigned_to_user_id = $${base-2}
        ${legacy}
        OR r.meeting_id IN (
             SELECT id FROM meetings WHERE assigned_to_user_id = $${base-1} OR created_by_user_id = $${base}
          )
      )`);
    }
    // Explicit assigned-to filter: allow OWNER/ADMIN any selectable, EMPLOYEE only self
    if (assignedToUserId) {
      if (req.user && (req.user.role === 'OWNER' || req.user.role === 'ADMIN')) {
        const valid = await validateSelectableUserId(pool, String(assignedToUserId));
        if (!valid) return res.status(400).json({ error: 'Invalid assignedToUserId' });
        const ur = await pool.query('SELECT email, username, full_name FROM public.users WHERE id=$1', [valid]);
        const parts = ur.rows.length ? [ur.rows[0].email, ur.rows[0].username, ur.rows[0].full_name].filter(v => v && String(v).trim().length) : [];
        params.push(valid);
        const base = params.length;
        let legacy = '';
        if (parts.length) {
          const placeholders = parts.map((_, i) => `$${base + i + 1}`).join(',');
          params.push(...parts);
          legacy = ` OR r.assigned_to IN (${placeholders})`;
        }
        filters.push(`(r.assigned_to_user_id = $${base}${legacy})`);
      } else if (req.user && req.user.role === 'EMPLOYEE') {
        const createdBySelf = createdBy && String(createdBy).toLowerCase() === 'self';
        const allowedUserId = createdBySelf ? await validateSelectableUserId(pool, String(assignedToUserId)) : (String(assignedToUserId) === String(req.user.sub) ? req.user.sub : null);
        if (!allowedUserId) return res.status(403).json({ error: 'Forbidden assignedToUserId' });
        // filter (with legacy fallbacks)
        params.push(allowedUserId);
        const base = params.length;
        const ur = await pool.query('SELECT email, username, full_name FROM public.users WHERE id=$1', [allowedUserId]);
        const parts = ur.rows.length ? [ur.rows[0].email, ur.rows[0].username, ur.rows[0].full_name].filter(v => v && String(v).trim().length) : [];
        let legacy = '';
        if (parts.length) {
          const placeholders = parts.map((_, i) => `$${base + i + 1}`).join(',');
          params.push(...parts);
          legacy = ` OR r.assigned_to IN (${placeholders})`;
        }
        filters.push(`(r.assigned_to_user_id = $${base}${legacy})`);
      }
    }
    // createdBy=self filter (intersection)
    if (createdBy && String(createdBy).toLowerCase() === 'self') {
      const selfId = req.user && req.user.sub;
      const actor = getActor(req);
      if (selfId) {
        params.push(selfId);
        const base = params.length;
        let legacy = '';
        const ids = [req.user.email, req.user.username, req.user.full_name, actor].filter(v => v && String(v).trim().length);
        if (ids.length) {
          const ph = ids.map((_, i) => `$${base + i + 1}`).join(',');
          params.push(...ids);
          legacy = ` OR r.created_by IN (${ph})`;
        }
        filters.push(`(r.created_by_user_id = $${base}${legacy})`);
      } else if (actor) {
        params.push(actor); filters.push(`r.created_by = $${params.length}`);
      }
    }
    // Role-based scoping for EMPLOYEE
    if (req.user && req.user.role === 'EMPLOYEE') {
      const ids = [req.user.email, req.user.username, req.user.full_name].filter(x => x && String(x).trim().length);
      const userId = req.user.sub;
      const conds = [];
      if (userId) {
        // param order: [created_by_user_id, assigned_to_user_id, meeting.assigned_to_user_id, meeting.created_by_user_id]
        params.push(userId, userId, userId, userId);
        const base = params.length;
        conds.push(`r.created_by_user_id = $${base-3}`);
        conds.push(`r.assigned_to_user_id = $${base-2}`);
        conds.push(`r.meeting_id IN (SELECT id FROM meetings WHERE assigned_to_user_id = $${base-1} OR created_by_user_id = $${base})`);
      }
      if (ids.length) {
        const base = params.length;
        params.push(...ids);
        const placeholders = ids.map((_, i) => `$${base + i + 1}`).join(',');
        const base2 = params.length;
        params.push(...ids);
        const placeholders2 = ids.map((_, i) => `$${base2 + i + 1}`).join(',');
        conds.push(`r.created_by IN (${placeholders})`);
        conds.push(`r.assigned_to IN (${placeholders})`);
        conds.push(`r.meeting_id IN (SELECT id FROM meetings WHERE assigned_to IN (${placeholders}) OR created_by IN (${placeholders2}))`);
      } else {
        const actor = getActor(req);
        params.push(actor, actor);
        conds.push(`r.created_by = $${params.length-1}`);
        conds.push(`r.assigned_to = $${params.length-1}`);
        conds.push(`r.meeting_id IN (SELECT id FROM meetings WHERE assigned_to = $${params.length-1} OR created_by = $${params.length})`);
      }
      filters.push(`(${conds.join(' OR ')})`);
    }
    const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';
    const order = (String(sort||'').toLowerCase() === 'due_ts_asc') ? 'ORDER BY r.due_ts ASC NULLS LAST' : 'ORDER BY r.due_ts DESC NULLS LAST';
    const enableEmailAgg = (featureFlags && featureFlags.hasRemindersEmailAudit) || FEATURE_REMINDERS_AUDIT;
    const enableCallAgg = (featureFlags && featureFlags.hasRemindersCallAudit) || FEATURE_REMINDERS_AUDIT;
    const selectAgg = [
      enableEmailAgg ? "COALESCE(ra.cnt,0)::int AS emails_sent_attempts" : null,
      enableCallAgg ? "COALESCE(rc.cnt,0)::int AS calls_attempts" : null
    ].filter(Boolean).length ? (', ' + [
      enableEmailAgg ? "COALESCE(ra.cnt,0)::int AS emails_sent_attempts" : null,
      enableCallAgg ? "COALESCE(rc.cnt,0)::int AS calls_attempts" : null
    ].filter(Boolean).join(', ')) : '';
    const joinAgg = [
      enableEmailAgg ? `LEFT JOIN (
        SELECT reminder_id, COUNT(*)::int AS cnt
          FROM reminder_email_selected_audit
         WHERE status = 'SENT'
         GROUP BY reminder_id
      ) ra ON ra.reminder_id = r.id` : '',
      enableCallAgg ? `LEFT JOIN (
        SELECT reminder_id, (COUNT(*) FILTER (WHERE status = 'COMPLETED'))::int AS cnt
          FROM reminder_call_attempt_audit
         GROUP BY reminder_id
      ) rc ON rc.reminder_id = r.id` : ''
    ].filter(Boolean).join('\n      ');
    const sql = `
      SELECT r.*,
             uc.full_name AS created_by_full_name,
             uc.username AS created_by_username,
             uc.email   AS created_by_email,
             ua.full_name AS assigned_to_full_name,
             ua.username  AS assigned_to_username,
             ua.email     AS assigned_to_email
             ${selectAgg}
      FROM reminders r
      LEFT JOIN public.users uc ON uc.id = r.created_by_user_id
      LEFT JOIN public.users ua ON ua.id = r.assigned_to_user_id
      ${joinAgg}
      ${where}
      ${order}
      LIMIT ${s} OFFSET ${offset}
    `;
    const rows = await pool.query(sql, params);
    res.json({ items: rows.rows, page: p, pageSize: s });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});
app.post('/api/reminders', requireAuth, async (req, res) => {
  let { id, title, due_ts, notes, status, type, recipient_email, receiver_email, notify_at, person_name, phone, opportunity_id, meeting_id, createdByUserId, assigned_to, assignedToUserId } = req.body || {};
  // Basic validation
  title = (title || '').toString().trim();
  if (!due_ts) return res.status(400).json({ error: 'due_ts is required' });
  const dueDate = toLocalDate(due_ts);
  if (!dueDate) return res.status(400).json({ error: 'due_ts must be a valid date' });
  type = (type || '').toString().toUpperCase();
  if (!type) type = 'CALL';
  if (!['CALL','EMAIL','MEETING'].includes(type)) return res.status(400).json({ error: 'type must be CALL or EMAIL' });
  // If EMAIL, require a recipient
  // Normalize receiver email (new field), accept recipient_email for backward compat
  receiver_email = (receiver_email || recipient_email || '').toString().trim();
  if (type === 'EMAIL' && !receiver_email) return res.status(400).json({ error: 'receiver_email is required for EMAIL reminders' });
  if (!receiver_email) receiver_email = null;
  // For CALL: require phone; allow person_name for both
  if (type === 'CALL') {
    phone = (phone || '').toString().trim();
    if (!phone) return res.status(400).json({ error: 'phone is required for CALL reminders' });
  }
  // Compute notify_at if not provided: day-before at 10:00 AM local
  function computeNotifyAt(d) {
    const when = new Date(d.getTime());
    const n = new Date(when);
    n.setDate(n.getDate() - 1);
    n.setHours(10, 0, 0, 0);
    // If computed notify is in the past but due is still future, send soon
    const now = new Date();
    if (n.getTime() < now.getTime() && when.getTime() > now.getTime()) {
      const soon = new Date(now.getTime() + 60 * 1000);
      return soon;
    }
    return n;
  }
  let notifyAtDate = notify_at ? toLocalDate(notify_at) : computeNotifyAt(dueDate);
  if (!notifyAtDate) notifyAtDate = computeNotifyAt(dueDate);
  // Default status
  status = (status || 'PENDING').toString().toUpperCase();
  if (!id) id = Math.random().toString(36).slice(2, 10).toUpperCase();
  const actor = getActor(req);
  let actorUserId = req.user && req.user.sub;
  // Anyone may create on behalf of another selectable user via createdByUserId (selectable = OWNER/EMPLOYEE; ADMIN excluded)
  if (createdByUserId) {
    try {
      const valid = await validateSelectableUserId(pool, String(createdByUserId));
      if (!valid) return res.status(400).json({ error: 'Invalid createdByUserId' });
      actorUserId = valid;
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }
  // If linked to a meeting, ensure employee has visibility to that meeting when EMPLOYEE
  if (req.user && req.user.role === 'EMPLOYEE' && meeting_id) {
    const chk = await pool.query('SELECT 1 FROM meetings WHERE id=$1 AND (assigned_to_user_id=$2 OR created_by_user_id=$2 OR assigned_to=$3 OR created_by=$3)', [meeting_id, actorUserId, actor]);
    if (!chk.rows.length) return res.status(403).json({ error: 'Forbidden for this meeting' });
  }
  // Normalize assignee for reminders (optional; separate from creator). Validate if explicit userId provided.
  let assigneeUserId = null;
  let assigneeLabel = null;
  try {
    // Employees cannot assign to others; default to themselves
    if (req.user && req.user.role === 'EMPLOYEE' && !assignedToUserId && !assigned_to) {
      assigneeUserId = actorUserId;
      const rr = await pool.query('SELECT email, username, full_name FROM public.users WHERE id=$1', [actorUserId]);
      assigneeLabel = rr.rows.length ? pickDisplay(rr.rows[0]) : getActor(req);
    }
    if (assignedToUserId) {
      // Allow self-assignment for any role, including ADMIN
      if (req.user && String(assignedToUserId) === String(req.user.sub)) {
        assigneeUserId = req.user.sub;
        const rr = await pool.query('SELECT email, username, full_name FROM public.users WHERE id=$1', [assigneeUserId]);
        assigneeLabel = rr.rows.length ? pickDisplay(rr.rows[0]) : getActor(req);
      } else {
        const valid = await validateSelectableUserId(pool, String(assignedToUserId));
        if (!valid) return res.status(400).json({ error: 'Invalid assignedToUserId' });
        assigneeUserId = valid;
        const rr = await pool.query('SELECT email, username, full_name FROM public.users WHERE id=$1', [valid]);
        assigneeLabel = rr.rows.length ? pickDisplay(rr.rows[0]) : null;
      }
    } else if (assigned_to) {
      // Best-effort resolve by identifier
      const u = await resolveUserByIdentifier(assigned_to);
      if (u) {
        assigneeUserId = u.id;
        assigneeLabel = pickDisplay(u);
      } else {
        assigneeLabel = String(assigned_to).trim() || null;
      }
    }
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
  // Auto-compose a reasonable title if none given
  if (!title) {
    if (type === 'CALL') {
      title = `Call ${person_name ? person_name : ''}`.trim();
    } else if (type === 'EMAIL') {
      title = `Email ${person_name ? person_name : ''}`.trim();
    }
  }
  // Resolve client_name (and normalize opportunity_id via meeting if needed)
  let clientName = null;
  try {
    if (opportunity_id) {
      const r = await pool.query('SELECT client_name FROM opportunities WHERE opportunity_id=$1', [opportunity_id]);
      if (r.rows.length) clientName = r.rows[0].client_name || null;
    }
    if (!clientName && meeting_id) {
      const r = await pool.query(
        `SELECT o.client_name, m.opportunity_id AS opp
           FROM meetings m
           LEFT JOIN opportunities o ON o.opportunity_id = m.opportunity_id
          WHERE m.id = $1`
        , [meeting_id]
      );
      if (r.rows.length) {
        clientName = r.rows[0].client_name || null;
        if (!opportunity_id && r.rows[0].opp) opportunity_id = r.rows[0].opp;
      }
    }
  } catch (e) {
    // Non-fatal; proceed without client name
  }
  try {
    const result = await pool.query(
      'INSERT INTO reminders (id, title, due_ts, notes, status, type, notify_at, receiver_email, person_name, phone, opportunity_id, meeting_id, created_by, created_by_user_id, assigned_to, assigned_to_user_id, client_name) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17) RETURNING *',
      [id, title, normalizeLocal(dueDate), notes || null, status, type, normalizeLocal(notifyAtDate), receiver_email, person_name || null, phone || null, opportunity_id || null, meeting_id || null, actor, actorUserId, assigneeLabel, assigneeUserId, clientName]
    );
    // Audit (CREATE)
    try { await insertReminderAuditV2(pool, 'CREATE', null, result.rows[0], null, req); } catch (e) { /* best-effort */ }
    res.status(201).json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// Email preview for a meeting (returns HTML without sending)
// Body can be either { meetingId } or a full meeting object { title, clientName, personName, startsAt, endsAt, location, meetingLink }
app.post('/api/email/preview/meeting', requireAuth, async (req, res) => {
  try {
    let meeting = null;
    let clientEmail = null;
    if (req.body && req.body.meetingId) {
      const id = String(req.body.meetingId);
      const r = await pool.query(`
        SELECT m.id, m.subject, m.starts_at, m.location, m.person_name, m.meeting_link,
               m.customer_id, m.opportunity_id, m.contract_id,
               COALESCE(c.client_name, o.client_name) AS client_name
          FROM meetings m
          LEFT JOIN customers c ON c.customer_id = m.customer_id
          LEFT JOIN opportunities o ON o.opportunity_id = m.opportunity_id
         WHERE m.id = $1
         LIMIT 1
      `, [id]);
      if (!r.rows.length) return res.status(404).json({ error: 'Meeting not found' });
  const row = r.rows[0];
  meeting = buildMeetingDto({ id: row.id, subject: row.subject, clientName: row.client_name, personName: row.person_name, startsAt: row.starts_at, endsAt: null, location: row.location, meetingLink: row.meeting_link });
      // Resolve client email from unified profile (prefers customer, then contract)
      try {
        const oppId = row.opportunity_id;
        if (oppId) {
          const profile = await getUnifiedClientProfile(String(oppId), pool, req);
          clientEmail = (profile && profile.details && profile.details.email) || null;
        } else if (row.customer_id) {
          // fallback: derive opportunity from customer
          const c = await pool.query('SELECT opportunity_id, email FROM customers WHERE customer_id=$1 LIMIT 1', [row.customer_id]);
          clientEmail = (c.rows[0] && (c.rows[0].email || null)) || null;
        }
      } catch {}
    } else {
      const b = req.body || {};
      if (!b.title || !b.clientName || !b.startsAt) {
        return res.status(400).json({ error: 'title, clientName, and startsAt are required' });
      }
  meeting = buildMeetingDto({ id: b.id || 'preview', subject: b.title, clientName: b.clientName, personName: b.personName, startsAt: b.startsAt, endsAt: b.endsAt, location: b.location, meetingLink: (b.meetingLink || b.meeting_link) });
      try {
        // If opportunityId is provided, use it to lookup client email
        if (b.opportunityId) {
          const profile = await getUnifiedClientProfile(String(b.opportunityId), pool, req);
          clientEmail = (profile && profile.details && profile.details.email) || null;
        } else if (b.customerId) {
          const c = await pool.query('SELECT email, opportunity_id FROM customers WHERE customer_id=$1 LIMIT 1', [String(b.customerId)]);
          if (c.rows.length) {
            clientEmail = c.rows[0].email || null;
            if (!clientEmail && c.rows[0].opportunity_id) {
              const profile = await getUnifiedClientProfile(String(c.rows[0].opportunity_id), pool, req);
              clientEmail = (profile && profile.details && profile.details.email) || clientEmail;
            }
          }
        }
      } catch {}
    }
    const html = meetingEmailHtml(meeting);
    const subject = `Meeting: ${meeting.clientName} — ${meeting.dateText} ${meeting.timeText}`;
    const api = process.env.API_ORIGIN || '';
    const icsUrl = `${api}/api/meetings/${encodeURIComponent(meeting.id)}/ics`;
    const googleUrl = generateGoogleCalendarLink(meeting);
    res.json({ html, subject, icsUrl, googleUrl, clientEmail });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// Email preview for a selection of reminders (CALL/EMAIL)
// Body: { reminderIds: string[], includeClientEmails?: boolean }
app.post('/api/email/preview/reminders', requireAuth, async (req, res) => {
  try {
    const { reminderIds, includeClientEmails } = req.body || {};
    const ids = Array.isArray(reminderIds) ? reminderIds.filter(Boolean).map(String) : [];
    if (!ids.length) return res.status(400).json({ error: 'reminderIds is required' });

    // Fetch selected reminders
    const ph = ids.map((_, i) => `$${i + 1}`).join(',');
    const r = await pool.query(
      `SELECT id, title, type, status, due_ts, receiver_email, person_name, phone, opportunity_id, client_name, notes,
              created_by, created_by_user_id, assigned_to, assigned_to_user_id, meeting_id
         FROM reminders
        WHERE id IN (${ph})`,
      ids
    );
    const rows = r.rows || [];
    if (!rows.length) return res.status(404).json({ error: 'No reminders found' });

    // Visibility guard for EMPLOYEE: ensure each selected reminder is visible to current user
    if (req.user && req.user.role === 'EMPLOYEE') {
      const actor = getActor(req);
      const uid = req.user.sub;
      // All must satisfy visibility
      for (const x of rows) {
        const own = (x.created_by_user_id && String(x.created_by_user_id) === String(uid)) || (x.created_by && String(x.created_by) === String(actor));
        const assigned = (x.assigned_to_user_id && String(x.assigned_to_user_id) === String(uid)) || (x.assigned_to && String(x.assigned_to) === String(actor));
        let viaMeeting = false;
        if (x.meeting_id) {
          try {
            const m = await pool.query('SELECT 1 FROM meetings WHERE id=$1 AND (assigned_to_user_id=$2 OR created_by_user_id=$2 OR assigned_to=$3 OR created_by=$3) LIMIT 1', [x.meeting_id, uid, actor]);
            viaMeeting = m.rows.length > 0;
          } catch {}
        }
        if (!(own || assigned || viaMeeting)) {
          return res.status(403).json({ error: 'Forbidden for one or more reminders' });
        }
      }
    }

    // Build DTOs for template
    const items = rows.map(row => ({
      id: row.id,
      title: row.title,
      kind: row.type,
      when: row.due_ts,
      receiver_email: row.receiver_email,
      person_name: row.person_name,
      phone: row.phone,
      client_name: row.client_name || null,
      notes: row.notes || null
    }));

    // Calendar links for adding multiple reminders at once via a single ICS
  const api = (process.env.API_ORIGIN || `${req.protocol}://${req.get('host')}`).replace(/\/$/, '');
    const idsCsv = encodeURIComponent(ids.join(','));
    const icsUrl = `${api}/api/reminders/ics?ids=${idsCsv}`;
  // For consistency across clients and to avoid external fetch issues, always provide direct ICS download links
  const appleUrl = icsUrl;
  const teamsUrl = icsUrl;
  const outlookUrl = icsUrl;
  const googleUrl = icsUrl;

    const html = remindersEmailHtml({ items, calendar: { icsUrl, appleUrl, outlookUrl, teamsUrl, googleUrl } });
    // Subject: simple, date-based
    const now = new Date();
    const today = now.toLocaleDateString('en-IN', { day: '2-digit', month: 'short' }).replace(/\./g, '');
    const subject = `Follow-ups: Today & Tomorrow (${today})`;

    // Suggested recipients: include EMAIL reminders' receiver_email and optionally client emails from linked opportunities
    const emailsSet = new Set();
    for (const row of rows) {
      if (row.type === 'EMAIL' && row.receiver_email) emailsSet.add(String(row.receiver_email).trim());
    }
    if (includeClientEmails) {
      // look up opportunity client email where available
      for (const row of rows) {
        const oppId = row.opportunity_id;
        if (!oppId) continue;
        try {
          const profile = await getUnifiedClientProfile(String(oppId), pool, req);
          const email = profile && profile.details && profile.details.email;
          if (email) emailsSet.add(String(email).trim());
        } catch {}
      }
    }
    const suggestedTo = Array.from(emailsSet.values());
  res.json({ html, subject, suggestedTo, icsUrl, googleUrl });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// Send an email (generic) with optional ICS attachment generated from meetingId or meeting payload
// Body: { to, cc?, bcc?, subject, html, meetingId?, meeting? }
app.post('/api/email/send', requireAuth, async (req, res) => {
  try {
    let { to, cc, bcc, subject, html, meetingId, meeting, remindersIds } = req.body || {};
    const normList = (v) => {
      if (!v) return [];
      if (Array.isArray(v)) return v.filter(Boolean).map(x => String(x).trim()).filter(Boolean);
      return String(v).split(',').map(s => s.trim()).filter(Boolean);
    };
    const toList = normList(to);
    const ccList = normList(cc);
    const bccList = normList(bcc);
    if (!toList.length) return res.status(400).json({ error: 'to is required' });
    if (!subject || !html) return res.status(400).json({ error: 'subject and html are required' });

  // Optional ICS attachment
    const attachments = [];
    if (meetingId || (meeting && meeting.title && meeting.clientName && meeting.startsAt)) {
      let dto;
      if (meetingId) {
        const r = await pool.query(`
          SELECT m.id, m.subject, m.starts_at, m.location, m.person_name, m.meeting_link,
                 COALESCE(c.client_name, o.client_name) AS client_name
            FROM meetings m
            LEFT JOIN customers c ON c.customer_id = m.customer_id
            LEFT JOIN opportunities o ON o.opportunity_id = m.opportunity_id
           WHERE m.id = $1
           LIMIT 1
        `, [String(meetingId)]);
        if (r.rows.length) {
          const row = r.rows[0];
          dto = buildMeetingDto({ id: row.id, subject: row.subject, clientName: row.client_name, personName: row.person_name, startsAt: row.starts_at, endsAt: null, location: row.location, meetingLink: row.meeting_link });
        }
      } else {
        dto = buildMeetingDto({ id: meeting.id || 'meeting', subject: meeting.title, clientName: meeting.clientName, personName: meeting.personName, startsAt: meeting.startsAt, endsAt: meeting.endsAt, location: meeting.location, meetingLink: (meeting.meetingLink || meeting.meeting_link) });
      }
      if (dto) {
        try {
          const ics = await generateICS(dto);
          attachments.push({ filename: `meeting-${dto.id}.ics`, content: ics, contentType: 'text/calendar; charset=utf-8' });
        } catch (e) {
          console.warn('[email/send] ICS generation failed:', e.message);
        }
      }
    }

    // Optional multi-ICS for reminders selection
  const rIds = Array.isArray(remindersIds) ? remindersIds.filter(Boolean).map(String) : [];
  let reminderTypeById = null;
  const allRecipients = Array.from(new Set([...(toList||[]), ...(ccList||[]), ...(bccList||[])]));
  const sentCount = allRecipients.length;
  const { randomUUID } = require('crypto');
  const operationId = rIds.length ? (randomUUID ? randomUUID() : null) : null;
    if (rIds.length) {
      try {
        const ph = rIds.map((_, i) => `$${i + 1}`).join(',');
        const r = await pool.query(
          `SELECT id, title, type, notes, due_ts AS when, person_name, phone, client_name
             FROM reminders
            WHERE id IN (${ph})
            ORDER BY due_ts ASC NULLS LAST`, rIds);
        if (r.rows && r.rows.length) {
          // Build a map of reminder_id -> reminder.type for audit enrichment
          try {
            reminderTypeById = new Map(r.rows.map(row => [String(row.id), row.type || null]));
          } catch {}
          const ics = await generateICSMultiForReminders(r.rows);
          const today = new Date();
          const ymd = today.toISOString().slice(0,10).replace(/-/g,'');
          attachments.push({ filename: `reminders-${ymd}.ics`, content: ics, contentType: 'text/calendar; charset=utf-8' });
        }
      } catch (e) {
        console.warn('[email/send] reminders multi-ICS generation failed:', e.message);
      }
    }

    try {
      const info = await sendEmail({ to: toList, cc: ccList, bcc: bccList, subject, html, attachments });
      // Audit meeting email send if linked to a meeting
      try {
        if (meetingId) {
          const actor = getActor(req);
          const actorUserId = req.user && req.user.sub;
          await pool.query(
            `INSERT INTO meeting_email_audit (meeting_id, performed_by_user_id, performed_by, subject, to_recipients, cc_recipients, bcc_recipients, status, message_id, sent_count, recipients_detail)
             VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7::jsonb,'SENT',$8,$9,$10::jsonb)`,
            [String(meetingId), actorUserId || null, actor || null, subject || null, JSON.stringify(toList), JSON.stringify(ccList), JSON.stringify(bccList), info.messageId || null, sentCount, JSON.stringify(allRecipients.map(e => ({ email: e, status: 'SENT', messageId: info.messageId || null })))]
          );
        }
      } catch (ae) { console.warn('[meeting_email_audit] insert failed:', ae.message); }
      // Audit reminders Email Selected (single batch) — one row per reminder
      try {
        if (FEATURE_REMINDERS_AUDIT && rIds.length && operationId) {
          const actor = getActor(req);
          const actorUserId = req.user && req.user.sub;
          const tasks = rIds.map(rid => {
            const rtype = reminderTypeById && reminderTypeById.get(String(rid)) || null;
            return pool.query(
              `INSERT INTO reminder_email_selected_audit (operation_id, reminder_id, performed_by_user_id, performed_by, subject, to_recipients, cc_recipients, bcc_recipients, recipients_dedup, sent_count, status, message_id, reminder_type)
               VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8::jsonb,$9::jsonb,$10,'SENT',$11,$12)`,
              [operationId, String(rid), actorUserId || null, actor || null, subject || null, JSON.stringify(toList), JSON.stringify(ccList), JSON.stringify(bccList), JSON.stringify(allRecipients), sentCount, info.messageId || null, rtype]
            );
          });
          await Promise.allSettled(tasks);
        }
      } catch (ae) { console.warn('[reminder_email_selected_audit] insert failed:', ae.message); }
      return res.json({ ok: true, messageId: info.messageId });
    } catch (e) {
      // Log detailed error to server logs for diagnostics (avoids leaking secrets to clients)
      try {
        const code = e && e.code;
        const response = e && (e.response || e.responseCode || e.command || e.reason || e.stack || e.message);
        console.error('[email/send] sendMail failed:', code, response);
      } catch {}
      // Provide a user-friendly error for missing SMTP config
      const msg = e && e.code === 'SMTP_CONFIG_MISSING' ? e.message : (e.message || 'Email send failed');
      // Attempt to audit failure as well if tied to a meeting
      try {
        if (meetingId) {
          const actor = getActor(req);
          const actorUserId = req.user && req.user.sub;
          await pool.query(
            `INSERT INTO meeting_email_audit (meeting_id, performed_by_user_id, performed_by, subject, to_recipients, cc_recipients, bcc_recipients, status, error, sent_count, recipients_detail)
             VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7::jsonb,'FAILED',$8,$9,$10::jsonb)`,
            [String(meetingId), actorUserId || null, actor || null, subject || null, JSON.stringify(toList), JSON.stringify(ccList), JSON.stringify(bccList), String(msg), 0, JSON.stringify(allRecipients.map(em => ({ email: em, status: 'FAILED', error: String(msg) })))]
          );
        }
      } catch (ae) { console.warn('[meeting_email_audit] insert failed:', ae.message); }
      // Audit reminders Email Selected failure (batch)
      try {
        if (FEATURE_REMINDERS_AUDIT && rIds.length && operationId) {
          const actor = getActor(req);
          const actorUserId = req.user && req.user.sub;
          const tasks = rIds.map(rid => {
            const rtype = reminderTypeById && reminderTypeById.get(String(rid)) || null;
            return pool.query(
              `INSERT INTO reminder_email_selected_audit (operation_id, reminder_id, performed_by_user_id, performed_by, subject, to_recipients, cc_recipients, bcc_recipients, recipients_dedup, sent_count, status, error, reminder_type)
               VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8::jsonb,$9::jsonb,$10,'FAILED',$11,$12)`,
              [operationId, String(rid), actorUserId || null, actor || null, subject || null, JSON.stringify(toList), JSON.stringify(ccList), JSON.stringify(bccList), JSON.stringify(allRecipients), 0, String(msg), rtype]
            );
          });
          await Promise.allSettled(tasks);
        }
      } catch (ae) { console.warn('[reminder_email_selected_audit] insert failed:', ae.message); }
      return res.status(500).json({ error: msg });
    }
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// Read meeting audits (v2) for a specific meeting
app.get('/api/meetings/:id/audit-v2', requireAuth, async (req, res) => {
  try {
    const id = String(req.params.id);
    // Visibility guard for EMPLOYEE
    if (req.user && req.user.role === 'EMPLOYEE') {
      const actor = getActor(req);
      const r = await pool.query('SELECT 1 FROM meetings WHERE id=$1 AND (assigned_to_user_id=$2 OR created_by_user_id=$2 OR assigned_to=$3 OR created_by=$3) LIMIT 1', [id, req.user.sub, actor]);
      if (!r.rows.length) return res.status(403).json({ error: 'Forbidden' });
    }
    const rows = await pool.query('SELECT * FROM meetings_audit_v2 WHERE meeting_id=$1 ORDER BY version ASC', [id]);
    res.json({ items: rows.rows });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// Read meeting email send audit for a specific meeting
app.get('/api/meetings/:id/email-audit', requireAuth, async (req, res) => {
  try {
    const id = String(req.params.id);
    // Visibility guard for EMPLOYEE
    if (req.user && req.user.role === 'EMPLOYEE') {
      const actor = getActor(req);
      const r = await pool.query('SELECT 1 FROM meetings WHERE id=$1 AND (assigned_to_user_id=$2 OR created_by_user_id=$2 OR assigned_to=$3 OR created_by=$3) LIMIT 1', [id, req.user.sub, actor]);
      if (!r.rows.length) return res.status(403).json({ error: 'Forbidden' });
    }
    const rows = await pool.query('SELECT * FROM meeting_email_audit WHERE meeting_id=$1 ORDER BY performed_at DESC', [id]);
    res.json({ items: rows.rows });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// List all meetings audit v2 entries with optional filters (meetingId substring, action, date range)
app.get('/api/meetings-audit-v2', requireAuth, async (req, res) => {
  try {
    let { meetingId, action, dateFrom, dateTo, page = 1, pageSize = 50 } = req.query || {};
    const p = clampInt(page, 1, 100000, 1);
    const s = clampInt(pageSize, 1, 500, 50);
    const offset = (p - 1) * s;
    const params = [];
    const filters = [];
    if (meetingId) {
      params.push(`%${String(meetingId).trim()}%`);
      filters.push(`a.meeting_id ILIKE $${params.length}`);
    }
    if (action) {
      const list = String(action).split(',').map(x => x.trim().toUpperCase()).filter(Boolean);
      if (list.length) {
        const ph = list.map((_, i) => `$${params.length + i + 1}`).join(',');
        params.push(...list);
        filters.push(`a.action IN (${ph})`);
      }
    }
    if (dateFrom && isValidDateTimeString(dateFrom)) { params.push(dateFrom); filters.push(`a.performed_at >= $${params.length}`); }
    if (dateTo && isValidDateTimeString(dateTo)) { params.push(dateTo); filters.push(`a.performed_at <= $${params.length}`); }

    // Visibility guard for EMPLOYEE: restrict to meetings they can see
    if (req.user && req.user.role === 'EMPLOYEE') {
      const actor = getActor(req);
      const uid = req.user.sub;
      params.push(uid, uid, actor, actor);
      const base = params.length;
      filters.push(`(m.assigned_to_user_id = $${base-3} OR m.created_by_user_id = $${base-2} OR m.assigned_to = $${base-1} OR m.created_by = $${base})`);
    }

    const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';
    const sql = `
      SELECT a.*
        FROM meetings_audit_v2 a
        LEFT JOIN meetings m ON m.id = a.meeting_id
        ${where}
        ORDER BY a.performed_at DESC
        LIMIT ${s} OFFSET ${offset}
    `;
    const rows = await pool.query(sql, params);
    res.json({ items: rows.rows, page: p, pageSize: s });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// --- Reminders audits: read endpoints ---
app.get('/api/reminders/:id/audit-v2', requireAuth, async (req, res) => {
  if (!FEATURE_REMINDERS_AUDIT) return res.status(404).json({ error: 'Not found' });
  try {
    const id = String(req.params.id);
    // Visibility for EMPLOYEE: created_by_user_id or assigned_to_user_id or via linked meeting
    if (req.user && req.user.role === 'EMPLOYEE') {
      const actor = getActor(req);
      const uid = req.user.sub;
      const r = await pool.query('SELECT created_by_user_id, created_by, assigned_to_user_id, assigned_to, meeting_id FROM reminders WHERE id=$1 LIMIT 1', [id]);
      if (!r.rows.length) return res.status(404).json({ error: 'Not found' });
      const row = r.rows[0];
      const own = (row.created_by_user_id && String(row.created_by_user_id) === String(uid)) || (row.created_by && String(row.created_by) === String(actor));
      const assigned = (row.assigned_to_user_id && String(row.assigned_to_user_id) === String(uid)) || (row.assigned_to && String(row.assigned_to) === String(actor));
      let viaMeeting = false;
      if (row.meeting_id) {
        const m = await pool.query('SELECT 1 FROM meetings WHERE id=$1 AND (assigned_to_user_id=$2 OR created_by_user_id=$2 OR assigned_to=$3 OR created_by=$3) LIMIT 1', [row.meeting_id, uid, actor]);
        viaMeeting = m.rows.length > 0;
      }
      if (!(own || assigned || viaMeeting)) return res.status(403).json({ error: 'Forbidden' });
    }
    const rows = await pool.query('SELECT * FROM reminders_audit_v2 WHERE reminder_id=$1 ORDER BY version ASC', [id]);
    res.json({ items: rows.rows });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/reminders/:id/email-audit', requireAuth, async (req, res) => {
  if (!FEATURE_REMINDERS_AUDIT) return res.status(404).json({ error: 'Not found' });
  try {
    const id = String(req.params.id);
    // Visibility for EMPLOYEE similar to above
    if (req.user && req.user.role === 'EMPLOYEE') {
      const actor = getActor(req);
      const uid = req.user.sub;
      const r = await pool.query('SELECT created_by_user_id, created_by, assigned_to_user_id, assigned_to, meeting_id FROM reminders WHERE id=$1 LIMIT 1', [id]);
      if (!r.rows.length) return res.status(404).json({ error: 'Not found' });
      const row = r.rows[0];
      const own = (row.created_by_user_id && String(row.created_by_user_id) === String(uid)) || (row.created_by && String(row.created_by) === String(actor));
      const assigned = (row.assigned_to_user_id && String(row.assigned_to_user_id) === String(uid)) || (row.assigned_to && String(row.assigned_to) === String(actor));
      let viaMeeting = false;
      if (row.meeting_id) {
        const m = await pool.query('SELECT 1 FROM meetings WHERE id=$1 AND (assigned_to_user_id=$2 OR created_by_user_id=$2 OR assigned_to=$3 OR created_by=$3) LIMIT 1', [row.meeting_id, uid, actor]);
        viaMeeting = m.rows.length > 0;
      }
      if (!(own || assigned || viaMeeting)) return res.status(403).json({ error: 'Forbidden' });
    }
    const rows = await pool.query('SELECT * FROM reminder_email_selected_audit WHERE reminder_id=$1 ORDER BY performed_at DESC', [id]);
    res.json({ items: rows.rows });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// Read call attempt audit for a specific reminder
app.get('/api/reminders/:id/call-audit', requireAuth, async (req, res) => {
  try {
    const id = String(req.params.id);
    // Visibility for EMPLOYEE mirrors email-audit
    if (req.user && req.user.role === 'EMPLOYEE') {
      const actor = getActor(req);
      const uid = req.user.sub;
      const r = await pool.query('SELECT created_by_user_id, created_by, assigned_to_user_id, assigned_to, meeting_id FROM reminders WHERE id=$1 LIMIT 1', [id]);
      if (!r.rows.length) return res.status(404).json({ error: 'Not found' });
      const row = r.rows[0];
      const own = (row.created_by_user_id && String(row.created_by_user_id) === String(uid)) || (row.created_by && String(row.created_by) === String(actor));
      const assigned = (row.assigned_to_user_id && String(row.assigned_to_user_id) === String(uid)) || (row.assigned_to && String(row.assigned_to) === String(actor));
      let viaMeeting = false;
      if (row.meeting_id) {
        const m = await pool.query('SELECT 1 FROM meetings WHERE id=$1 AND (assigned_to_user_id=$2 OR created_by_user_id=$2 OR assigned_to=$3 OR created_by=$3) LIMIT 1', [row.meeting_id, uid, actor]);
        viaMeeting = m.rows.length > 0;
      }
      if (!(own || assigned || viaMeeting)) return res.status(403).json({ error: 'Forbidden' });
    }
    // If table not present, return empty array gracefully
    try {
      const rows = await pool.query('SELECT * FROM reminder_call_attempt_audit WHERE reminder_id=$1 ORDER BY performed_at DESC', [id]);
      res.json({ items: rows.rows });
    } catch (e) {
      // Table likely missing; degrade
      res.json({ items: [] });
    }
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// Global listings with filters
app.get('/api/reminders-audit-v2', requireAuth, async (req, res) => {
  if (!FEATURE_REMINDERS_AUDIT) return res.status(404).json({ error: 'Not found' });
  try {
    let { reminderId, action, dateFrom, dateTo, page = 1, pageSize = 50 } = req.query || {};
    const p = clampInt(page, 1, 100000, 1);
    const s = clampInt(pageSize, 1, 500, 50);
    const offset = (p - 1) * s;
    const params = [];
    const filters = [];
    if (reminderId) {
      params.push(`%${String(reminderId).trim()}%`);
      filters.push(`a.reminder_id ILIKE $${params.length}`);
    }
    if (action) {
      const list = String(action).split(',').map(x => x.trim().toUpperCase()).filter(Boolean);
      if (list.length) {
        const ph = list.map((_, i) => `$${params.length + i + 1}`).join(',');
        params.push(...list);
        filters.push(`a.action IN (${ph})`);
      }
    }
    if (dateFrom && isValidDateTimeString(dateFrom)) { params.push(dateFrom); filters.push(`a.performed_at >= $${params.length}`); }
    if (dateTo && isValidDateTimeString(dateTo)) { params.push(dateTo); filters.push(`a.performed_at <= $${params.length}`); }

    // Visibility guard for EMPLOYEE using reminders and linked meeting
    if (req.user && req.user.role === 'EMPLOYEE') {
      const actor = getActor(req);
      const uid = req.user.sub;
      params.push(uid, uid, actor, actor);
      const base = params.length;
      filters.push(`(
        r.created_by_user_id = $${base-3} OR r.assigned_to_user_id = $${base-2} OR r.created_by = $${base-1} OR r.assigned_to = $${base}
        OR (r.meeting_id IS NOT NULL AND EXISTS (
              SELECT 1 FROM meetings m WHERE m.id = r.meeting_id AND (m.assigned_to_user_id = $${base-3} OR m.created_by_user_id = $${base-3} OR m.assigned_to = $${base-1} OR m.created_by = $${base})
            ))
      )`);
    }

    const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';
    const sql = `
      SELECT a.*
        FROM reminders_audit_v2 a
        LEFT JOIN reminders r ON r.id = a.reminder_id
        ${where}
        ORDER BY a.performed_at DESC
        LIMIT ${s} OFFSET ${offset}
    `;
    const rows = await pool.query(sql, params);
    res.json({ items: rows.rows, page: p, pageSize: s });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/reminders-email-selected-audit', requireAuth, async (req, res) => {
  if (!FEATURE_REMINDERS_AUDIT) return res.status(404).json({ error: 'Not found' });
  try {
    let { operationId, reminderId, status, page = 1, pageSize = 50 } = req.query || {};
    const p = clampInt(page, 1, 100000, 1);
    const s = clampInt(pageSize, 1, 500, 50);
    const offset = (p - 1) * s;
    const params = [];
    const filters = [];
    if (operationId) { params.push(String(operationId)); filters.push(`a.operation_id = $${params.length}`); }
    if (reminderId) { params.push(String(reminderId)); filters.push(`a.reminder_id = $${params.length}`); }
    if (status) {
      const list = String(status).split(',').map(x => x.trim().toUpperCase()).filter(Boolean);
      if (list.length) {
        const ph = list.map((_, i) => `$${params.length + i + 1}`).join(',');
        params.push(...list);
        filters.push(`a.status IN (${ph})`);
      }
    }
    // Visibility for EMPLOYEE
    if (req.user && req.user.role === 'EMPLOYEE') {
      const actor = getActor(req);
      const uid = req.user.sub;
      params.push(uid, uid, actor, actor);
      const base = params.length;
      filters.push(`(
        r.created_by_user_id = $${base-3} OR r.assigned_to_user_id = $${base-2} OR r.created_by = $${base-1} OR r.assigned_to = $${base}
        OR (r.meeting_id IS NOT NULL AND EXISTS (
              SELECT 1 FROM meetings m WHERE m.id = r.meeting_id AND (m.assigned_to_user_id = $${base-3} OR m.created_by_user_id = $${base-3} OR m.assigned_to = $${base-1} OR m.created_by = $${base})
            ))
      )`);
    }

    const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';
    const sql = `
      SELECT a.*
        FROM reminder_email_selected_audit a
        LEFT JOIN reminders r ON r.id = a.reminder_id
        ${where}
        ORDER BY a.performed_at DESC
        LIMIT ${s} OFFSET ${offset}
    `;
    const rows = await pool.query(sql, params);
    res.json({ items: rows.rows, page: p, pageSize: s });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// Edit reminder fields (CALL/EMAIL)
app.put('/api/reminders/:id', requireAuth, async (req, res) => {
  const id = req.params.id;
  if (!id) return res.status(400).json({ error: 'id is required' });
  try {
  const cur = await pool.query('SELECT * FROM reminders WHERE id=$1', [id]);
    if (cur.rows.length === 0) return res.status(404).json({ error: 'Not found' });
    // Guard: Employees can edit only reminders they created
    if (req.user && req.user.role === 'EMPLOYEE') {
      const actor = getActor(req);
      if (cur.rows[0].created_by_user_id !== req.user.sub && String(cur.rows[0].created_by || '') !== String(actor)) {
        return res.status(403).json({ error: 'Employees can edit only reminders they created' });
      }
    }
    const row = cur.rows[0];
    const type = String(row.type || '').toUpperCase();
    let { title, due_ts, notes, status, receiver_email, recipient_email, person_name, phone, notify_at, assignedToUserId, assigned_to } = req.body || {};
    title = title !== undefined ? String(title || '').trim() : row.title;
    notes = notes !== undefined ? (notes || null) : row.notes;
    // Parse dates if provided (as LOCAL time)
    let dueDate = row.due_ts ? toLocalDate(row.due_ts) : null;
    if (due_ts !== undefined) {
      const d = toLocalDate(due_ts);
      if (!d) return res.status(400).json({ error: 'due_ts must be a valid date' });
      dueDate = d;
    }
    let notifyAtDate = row.notify_at ? toLocalDate(row.notify_at) : null;
    if (notify_at !== undefined) {
      const n = toLocalDate(notify_at);
      if (!n) return res.status(400).json({ error: 'notify_at must be a valid date' });
      notifyAtDate = n;
    }
    function computeNotifyAt(d) {
      const when = new Date(d.getTime());
      const n = new Date(when);
      n.setDate(n.getDate() - 1);
      n.setHours(10, 0, 0, 0);
      const now = new Date();
      if (n.getTime() < now.getTime() && when.getTime() > now.getTime()) {
        return new Date(now.getTime() + 60 * 1000);
      }
      return n;
    }
    // If due changed and notify not explicitly provided, recompute
    if (due_ts !== undefined && notify_at === undefined) {
      notifyAtDate = computeNotifyAt(dueDate);
    }
    // Normalize receiver email from either field
    receiver_email = receiver_email !== undefined ? String(receiver_email || '').trim() : row.receiver_email;
    if ((receiver_email === '' || receiver_email === undefined) && recipient_email !== undefined) {
      receiver_email = String(recipient_email || '').trim();
    }
    person_name = person_name !== undefined ? (person_name || null) : row.person_name;
    phone = phone !== undefined ? String(phone || '') : row.phone;
    status = status !== undefined ? String(status || '').toUpperCase() : row.status;
    const allowed = new Set(['PENDING','DONE','SENT','FAILED']);
    if (status && !allowed.has(status)) return res.status(400).json({ error: 'Invalid status' });
    // Type-specific validation
    if (type === 'EMAIL') {
      if (!receiver_email) return res.status(400).json({ error: 'receiver_email is required for EMAIL reminders' });
    }
    if (type === 'CALL') {
      if (!phone) return res.status(400).json({ error: 'phone is required for CALL reminders' });
    }
    // Normalize and optionally update assignee
    // Policy: The creator (any role) may reassign their own reminder to any selectable user.
    // OWNER/ADMIN may reassign any reminder. EMPLOYEE is already restricted above to only edit reminders they created.
    let newAssignedUserId = row.assigned_to_user_id || null;
    let newAssignedLabel = row.assigned_to || null;
    if (assignedToUserId !== undefined && assignedToUserId) {
      const valid = await validateSelectableUserId(pool, String(assignedToUserId));
      if (!valid) return res.status(400).json({ error: 'Invalid assignedToUserId' });
      newAssignedUserId = valid;
      const rr = await pool.query('SELECT email, username, full_name FROM public.users WHERE id=$1', [valid]);
      newAssignedLabel = rr.rows.length ? pickDisplay(rr.rows[0]) : newAssignedLabel;
    } else if (assigned_to !== undefined) {
      if (assigned_to === null) {
        newAssignedUserId = null;
        newAssignedLabel = null;
      } else {
        const u = await resolveUserByIdentifier(assigned_to);
        if (u) {
          newAssignedUserId = u.id;
          newAssignedLabel = pickDisplay(u);
        } else {
          newAssignedLabel = String(assigned_to).trim() || null;
        }
      }
    }
    // Build update
    const updated = await pool.query(
      `UPDATE reminders
       SET title=$1, due_ts=$2, notes=$3, status=$4, notify_at=$5, receiver_email=$6, person_name=$7, phone=$8, assigned_to=$9, assigned_to_user_id=$10
       WHERE id=$11 RETURNING *`,
      [title, dueDate ? normalizeLocal(dueDate) : row.due_ts, notes, status, notifyAtDate ? normalizeLocal(notifyAtDate) : row.notify_at, receiver_email || null, person_name, phone || null, newAssignedLabel, newAssignedUserId, id]
    );
    // Audit (UPDATE)
    try { await insertReminderAuditV2(pool, 'UPDATE', row, updated.rows[0], null, req); } catch (e) { /* best-effort */ }
    res.json(updated.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// Update reminder (e.g., mark DONE/SENT)
app.patch('/api/reminders/:id', requireAuth, async (req, res) => {
  try {
    const id = req.params.id;
    const { status } = req.body || {};
    if (!id) return res.status(400).json({ error: 'id is required' });
    const allowed = new Set(['PENDING','DONE','SENT','FAILED']);
    if (status && !allowed.has(String(status).toUpperCase())) {
      return res.status(400).json({ error: 'Invalid status' });
    }
    if (!status) {
      return res.status(400).json({ error: 'Nothing to update' });
    }
    // Guard: Employees can update only reminders they created
    let beforeRow = null;
    {
      const cur = await pool.query('SELECT * FROM reminders WHERE id=$1', [id]);
      if (!cur.rows.length) return res.status(404).json({ error: 'Not found' });
      beforeRow = cur.rows[0];
      if (req.user && req.user.role === 'EMPLOYEE') {
        const actor = getActor(req);
        const isCreator = (beforeRow.created_by_user_id === req.user.sub) || (String(beforeRow.created_by || '') === String(actor));
        const isAssigneeSelf = (beforeRow.assigned_to_user_id && String(beforeRow.assigned_to_user_id) === String(req.user.sub)) || (beforeRow.assigned_to && String(beforeRow.assigned_to) === String(actor));
        const newStatus = String(status).toUpperCase();
        const isTerminal = ['DONE','SENT','FAILED'].includes(String(beforeRow.status || '').toUpperCase());
        const isAllowedRevert = isAssigneeSelf && newStatus === 'PENDING' && isTerminal;
        if (!isCreator && !isAllowedRevert) {
          return res.status(403).json({ error: 'Employees can update only reminders they created (or revert assigned reminders to PENDING)' });
        }
      }
    }
    const newStatus = String(status).toUpperCase();
    const r = await pool.query('UPDATE reminders SET status=$1 WHERE id=$2 RETURNING *', [newStatus, id]);
    if (r.rows.length === 0) return res.status(404).json({ error: 'Not found' });
    // Audit (STATUS)
    try {
      let auditNote = null;
      const wasTerminal = ['DONE','SENT','FAILED'].includes(String(beforeRow.status || '').toUpperCase());
      if (newStatus === 'PENDING' && wasTerminal) {
        auditNote = 'revert_to_pending';
      }
      await insertReminderAuditV2(pool, 'STATUS', beforeRow, r.rows[0], auditNote, req);
    } catch (e) { /* best-effort */ }
    // If this is a CALL reminder, also record a call attempt audit row (COMPLETED/FAILED)
    try {
      if (beforeRow && String(beforeRow.type || '').toUpperCase() === 'CALL') {
        const actor = getActor(req);
        const actorUserId = req.user && req.user.sub;
        // Map reminder status to call attempt status
        let callStatus = null;
        if (newStatus === 'DONE') callStatus = 'COMPLETED';
        else if (newStatus === 'FAILED') callStatus = 'FAILED';
        else if (newStatus === 'PENDING') callStatus = 'INITIATED';
        if (callStatus) {
          // best-effort insert; table presence governed by feature flag
          await pool.query(
            `INSERT INTO reminder_call_attempt_audit (reminder_id, performed_by_user_id, performed_by, phone, status)
             VALUES ($1,$2,$3,$4,$5)`,
            [String(id), actorUserId || null, actor || null, beforeRow.phone || null, callStatus]
          );
        }
      }
    } catch (e) {
      // non-fatal
      if (!process.env.SUPPRESS_DB_LOG) console.warn('[call_attempt_audit] insert failed:', e.message);
    }
    res.json(r.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// Lightweight client lookup for Meetings dropdown
// Returns client_name, opportunity_id, customer_id, and an ACTIVE contract_id if any
app.get('/api/clients-lookup', async (req, res) => {
  try {
    const { q, limit } = req.query;
    const lim = Math.min(Math.max(Number(limit) || 20, 1), 100);
    const params = [];
    const hasQ = q && String(q).trim().length;
    if (hasQ) {
      const like = `%${String(q).toLowerCase()}%`;
      params.push(like); // client_name
      params.push(like); // opportunity_id
    }
    const qFilter = hasQ ? `WHERE (LOWER(o.client_name) LIKE $1 OR LOWER(o.opportunity_id) LIKE $2)` : '';
    const sql = `
      WITH best_customer AS (
        SELECT DISTINCT ON (opportunity_id) opportunity_id, customer_id
        FROM customers
        ORDER BY opportunity_id, (customer_status = 'ACTIVE') DESC, created_at DESC
      ), active_contract AS (
        SELECT DISTINCT ON (opportunity_id) opportunity_id, contract_id
        FROM contracts
        WHERE contract_status = 'ACTIVE'
        ORDER BY opportunity_id, created_at DESC
      )
      SELECT 'OPPORTUNITY' AS entity_type,
             o.client_name,
             o.opportunity_id,
             bc.customer_id,
             ac.contract_id
      FROM opportunities o
      LEFT JOIN best_customer bc ON bc.opportunity_id = o.opportunity_id
      LEFT JOIN active_contract ac ON ac.opportunity_id = o.opportunity_id
      ${qFilter}
      ORDER BY o.client_name ASC
      LIMIT ${lim}
    `;
    const r = await pool.query(sql, params);
    res.json(r.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// Lightweight users lookup for dropdowns (e.g., salesperson). Returns active users with minimal fields.
// Query params:
//   roles: optional comma-separated roles to include (defaults to OWNER,EMPLOYEE)
//   q: optional search on full_name/username/email
app.get('/api/users-lookup', async (req, res) => {
  try {
    const rolesParam = String(req.query.roles || 'OWNER,EMPLOYEE');
    const roles = rolesParam.split(',').map(r => r.trim().toUpperCase()).filter(Boolean);
    const params = [];
    const filters = ["u.active = TRUE"];   
    if (roles.length) {
      const ph = roles.map((_, i) => `$${params.length + i + 1}`).join(',');
      params.push(...roles);
      filters.push(`u.role IN (${ph})`);
    }
    if (req.query.q) {
      const like = `%${String(req.query.q).toLowerCase()}%`;
      params.push(like, like, like);
      filters.push(`(LOWER(COALESCE(u.full_name,'')) LIKE $${params.length-2} OR LOWER(COALESCE(u.username,'')) LIKE $${params.length-1} OR LOWER(COALESCE(u.email,'')) LIKE $${params.length})`);
    }
    const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';
    const sql = `SELECT id, full_name, username, email, role FROM public.users u ${where} ORDER BY role, COALESCE(full_name, username, email)`;
    const r = await pool.query(sql, params);
    res.json(r.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// Employee overview: fetch a specific user's upcoming meetings and reminders
// Only OWNER/ADMIN may access this endpoint. EMPLOYEE gets 403.
// Query: userId (required), from (optional, defaults to start of today), to (optional)
app.get('/api/employee-overview', requireAuth, requireRole('OWNER','ADMIN'), async (req, res) => {
  try {
    const userId = String(req.query.userId || '').trim();
    if (!userId) return res.status(400).json({ error: 'userId is required' });
    // Validate user exists and is active and is OWNER/EMPLOYEE (exclude ADMIN targets)
    const ru = await pool.query(
      `SELECT id, email, username, full_name, role, active
         FROM public.users
        WHERE id=$1 AND active=TRUE AND role IN ('OWNER','EMPLOYEE')`,
      [userId]
    );
    if (!ru.rows.length) return res.status(404).json({ error: 'User not found or not selectable' });

    // Parse date range; default from = start of today local as SQL
    function toLocalStartOfDaySQL() {
      const d = new Date();
      d.setHours(0,0,0,0);
      const pad = n => String(n).padStart(2,'0');
      return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} 00:00:00`;
    }
    const from = req.query.from ? String(req.query.from) : toLocalStartOfDaySQL();
    const to = req.query.to ? String(req.query.to) : null;

    // Meetings for this user: assigned_to_user_id or created_by_user_id
    const mParams = [userId, userId, from];
  const mFilters = ['(m.assigned_to_user_id = $1 OR m.created_by_user_id = $2)', "m.status IN ('SCHEDULED','RESCHEDULED')", 'm.starts_at >= $3'];
    if (to) { mParams.push(to); mFilters.push(`m.starts_at <= $${mParams.length}`); }
    const meetingsSql = `
      SELECT m.*,
             ua.full_name AS assigned_to_full_name,
             ua.username AS assigned_to_username,
             ua.email AS assigned_to_email,
             uc.full_name AS created_by_full_name,
             uc.username AS created_by_username,
             uc.email AS created_by_email
        FROM meetings m
        LEFT JOIN public.users ua ON ua.id = m.assigned_to_user_id
        LEFT JOIN public.users uc ON uc.id = m.created_by_user_id
       WHERE ${mFilters.join(' AND ')}
       ORDER BY m.starts_at ASC NULLS LAST
       LIMIT 1000
    `;

    // Reminders for this user: created_by_user_id OR linked to meetings for this user
    const rParams = [userId, userId, userId, from];
    const rFilters = [
      `(
         r.created_by_user_id = $1
         OR r.assigned_to_user_id = $2
         OR r.meeting_id IN (
              SELECT id FROM meetings m2
               WHERE m2.assigned_to_user_id = $3 OR m2.created_by_user_id = $3
           )
       )`,
      "r.type IN ('CALL','EMAIL')",
      "r.status = 'PENDING'",
      'r.due_ts >= $4'
    ];
    if (to) { rParams.push(to); rFilters.push(`r.due_ts <= $${rParams.length}`); }
    const remindersSql = `
      SELECT r.*
        FROM reminders r
       WHERE ${rFilters.join(' AND ')}
       ORDER BY r.due_ts ASC NULLS LAST
       LIMIT 1000
    `;

    const [mRes, rRes] = await Promise.all([
      pool.query(meetingsSql, mParams),
      pool.query(remindersSql, rParams)
    ]);
    res.json({ meetings: mRes.rows, reminders: rRes.rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// Optional: auto-run schema migration at boot when enabled via env
async function autoMigrateIfEnabled() {
  const flag = String(process.env.AUTO_MIGRATE || process.env.MIGRATE_ON_START || '').toLowerCase();
  const enabled = flag === '1' || flag === 'true' || flag === 'yes' || flag === 'on';
  if (!enabled) return;
  try {
    const fs = require('fs');
    const path = require('path');
    const pool = require('./db');
    const schemaPath = path.join(__dirname, 'schema.sql');
    const sql = fs.readFileSync(schemaPath, 'utf8');
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(sql);
      await client.query('COMMIT');
      console.log('[auto-migrate] schema.sql applied successfully');
    } catch (e) {
      await client.query('ROLLBACK');
      console.error('[auto-migrate] failed:', e.message);
    } finally {
      client.release();
    }
  } catch (e) {
    console.error('[auto-migrate] error:', e.message);
  }
}

let server;
(async () => {
  await autoMigrateIfEnabled();
  server = app.listen(process.env.PORT || 5000, () => {
    console.log('Server running on port', process.env.PORT || 5000);
  });
})();
// Graceful shutdown to avoid data loss
function shutdown(signal) {
  console.log(`\n[${signal}] Shutting down gracefully...`);
  server.close(() => {
    try { require('./db').end && require('./db').end(); } catch {}
    process.exit(0);
  });
  setTimeout(() => process.exit(1), 10000).unref();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
// Global error handler
// Note: keep last
// eslint-disable-next-line no-unused-vars
app.use((err, req, res, next) => {
  console.error('[UnhandledError]', err);
  if (res.headersSent) return;
  res.status(500).json({ error: 'Internal server error' });
});

// ===========================
// Auth Endpoints (Phase 1)
// ===========================
// Register initial OWNER (only allowed if no owner exists)
app.post('/api/auth/register-initial', async (req, res) => {
  try {
    const { email, password, full_name } = req.body || {};
    if (!email || !password) return res.status(400).json({ error: 'email and password required' });
    if (await ownerExists()) return res.status(400).json({ error: 'Owner already exists' });
    const pwHash = await hashPassword(password);
    const newId = randomUUID();
    const uname = String(email).toLowerCase().split('@')[0];
    const r = await pool.query(
      'INSERT INTO public.users (id, email, username, full_name, role, password_hash, must_change_password) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id, email, username, full_name, role, created_at',
      [newId, String(email).toLowerCase(), uname, full_name || null, 'OWNER', pwHash, false]
    );
    const user = r.rows[0];
    const token = signToken(user);
    res.status(201).json({ user, token });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// Owner exists check
app.get('/api/auth/owner-exists', async (req, res) => {
  try {
    const exists = await ownerExists();
    res.json({ exists });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// Login (identifier = username or email)
app.post('/api/auth/login', async (req, res) => {
  try {
    const { identifier, email, password } = req.body || {};
    const idOrEmail = identifier || email;
    if (!idOrEmail || !password) return res.status(400).json({ error: 'identifier/email and password required' });
    // Single flexible lookup: accept either username OR email in one query (case-insensitive)
    const val = String(idOrEmail).trim();
    let r = await pool.query(
      'SELECT * FROM public.users WHERE active=TRUE AND (LOWER(username)=LOWER($1) OR LOWER(email)=LOWER($1))',
      [val]
    );
    // Optional: if still not found and identifier looks like a full name, try exact case-insensitive full_name match when unique
    if (r.rows.length === 0 && /\s/.test(val)) {
      const rf = await pool.query('SELECT * FROM public.users WHERE active=TRUE AND LOWER(full_name)=LOWER($1)', [val]);
      if (rf.rows.length === 1) {
        r = rf;
      }
    }
    if (r.rows.length === 0) return res.status(401).json({ error: 'Invalid credentials' });
    const user = r.rows[0];
    const ok = await verifyPassword(password, user.password_hash);
    if (!ok) return res.status(401).json({ error: 'Invalid credentials' });
    await pool.query('UPDATE public.users SET last_login=NOW() WHERE id=$1', [user.id]);
    const pub = { id: user.id, email: user.email, username: user.username, phone: user.phone, full_name: user.full_name, role: user.role, must_change_password: user.must_change_password };
    const token = signToken(pub);
    res.json({ user: pub, token, requirePasswordChange: !!user.must_change_password });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// Current user
app.get('/api/auth/me', requireAuth, async (req, res) => {
  try {
  const r = await pool.query('SELECT id, email, username, phone, full_name, role, must_change_password, created_at, last_login, active, joining_date, status FROM public.users WHERE id=$1', [req.user.sub]);
    if (r.rows.length === 0) return res.status(404).json({ error: 'Not found' });
    res.json(r.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// Change password (requires auth); supports first-login change and regular change
app.post('/api/auth/change-password', requireAuth, async (req, res) => {
  try {
    const userId = req.user.sub;
    const { currentPassword, newPassword } = req.body || {};
    if (!newPassword) return res.status(400).json({ error: 'newPassword required' });
  const r = await pool.query('SELECT id, password_hash, must_change_password FROM public.users WHERE id=$1 AND active=TRUE', [userId]);
    if (!r.rows.length) return res.status(404).json({ error: 'User not found' });
    const row = r.rows[0];
    // If not first login, require currentPassword
    if (!row.must_change_password) {
      if (!currentPassword) return res.status(400).json({ error: 'currentPassword required' });
      const ok = await verifyPassword(currentPassword, row.password_hash);
      if (!ok) return res.status(401).json({ error: 'Current password is incorrect' });
    }
    const hash = await hashPassword(newPassword);
  await pool.query('UPDATE public.users SET password_hash=$1, must_change_password=FALSE, last_password_change_at=NOW() WHERE id=$2', [hash, userId]);
    res.json({ success: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// Create user (OWNER or ADMIN). Role creation rules depend on creator role.
app.post('/api/users', requireAuth, requireRole('OWNER','ADMIN'), async (req, res) => {
  try {
    const { email, username, phone, password, role, full_name, joining_date, status } = req.body || {};
    // Enforce username mandatory (email optional)
    if (!username || !password || !role) return res.status(400).json({ error: 'username, password, role required' });
    const rRole = role.toUpperCase();
    // Creator-based role constraints
    const creator = req.user; // { sub, role }
    if (creator.role === 'OWNER') {
      // As requested: OWNER may create OWNER or EMPLOYEE only (no ADMIN)
      if (!['OWNER','EMPLOYEE'].includes(rRole)) return res.status(400).json({ error: 'OWNER can create only OWNER or EMPLOYEE' });
    } else if (creator.role === 'ADMIN') {
      // ADMIN may create OWNER, ADMIN, or EMPLOYEE
      if (!['OWNER','ADMIN','EMPLOYEE'].includes(rRole)) return res.status(400).json({ error: 'Invalid role' });
    } else {
      return res.status(403).json({ error: 'Forbidden' });
    }
    // Uniqueness checks (email and username if provided)
    if (email) {
      const exists = await pool.query('SELECT 1 FROM public.users WHERE email=$1', [String(email).toLowerCase()]);
      if (exists.rows.length) return res.status(409).json({ error: 'Email already in use' });
    }
    if (username) {
      const existsU = await pool.query('SELECT 1 FROM public.users WHERE LOWER(username)=LOWER($1)', [String(username)]);
      if (existsU.rows.length) return res.status(409).json({ error: 'Username already in use' });
    }
    const pwHash = await hashPassword(password);
    const newId = randomUUID();
    const ins = await pool.query(
      'INSERT INTO public.users (id, email, username, phone, full_name, role, password_hash, must_change_password, joining_date, status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id, email, username, phone, full_name, role, created_at, joining_date, status',
      [newId, email ? String(email).toLowerCase() : null, username || null, phone || null, full_name || null, rRole, pwHash, true, joining_date || null, status || 'ACTIVE']
    );
    res.status(201).json(ins.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// List users (OWNER or ADMIN). ADMIN can see all users, including OWNER.
app.get('/api/users', requireAuth, requireRole('OWNER','ADMIN'), async (req, res) => {
  try {
    const r = await pool.query('SELECT id, email, username, phone, full_name, role, active, created_at, last_login, joining_date, status FROM public.users ORDER BY role, COALESCE(username,email)');
    res.json(r.rows);
  } catch (err) {
    console.error(err); res.status(500).json({ error: err.message });
  }
});

// Update user fields (Owner/Admin)
// Supports: status, joining_date, full_name, email, phone, username, role (with creator-role constraints)
app.patch('/api/users/:id', requireAuth, requireRole('OWNER','ADMIN'), async (req, res) => {
  try {
    const id = req.params.id;
    let { status, joining_date, full_name, email, phone, username, role } = req.body || {};

    // Validate status (optional)
    const allowedStatus = new Set(['ACTIVE','INACTIVE','ON_LEAVE','SUSPENDED']);
    if (status !== undefined) {
      status = String(status).toUpperCase();
      if (!allowedStatus.has(status)) return res.status(400).json({ error: 'Invalid status' });
    }

    // Validate joining_date (optional) - must be YYYY-MM-DD
    if (joining_date !== undefined && joining_date !== null) {
      const s = String(joining_date);
      if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) {
        return res.status(400).json({ error: 'joining_date must be YYYY-MM-DD' });
      }
    }

    // Normalize email/username (optional)
    if (email !== undefined && email !== null) email = String(email).toLowerCase();
    if (username !== undefined && username !== null) username = String(username);

    // Role change rules follow creator constraints used in create
    if (role !== undefined && role !== null) {
      const newRole = String(role).toUpperCase();
      if (req.user.role === 'OWNER') {
        if (!['OWNER','EMPLOYEE'].includes(newRole)) return res.status(400).json({ error: 'OWNER can set role only to OWNER or EMPLOYEE' });
      } else if (req.user.role === 'ADMIN') {
        if (!['OWNER','ADMIN','EMPLOYEE'].includes(newRole)) return res.status(400).json({ error: 'Invalid role' });
      }
      role = newRole;
    }

    // Build dynamic update
    const sets = [];
    const params = [];
    const add = (sqlFragment, val) => { params.push(val); sets.push(sqlFragment.replace('$idx', `$${params.length}`)); };
    if (status !== undefined) add('status=$idx', status);
    if (joining_date !== undefined) add('joining_date=$idx', joining_date || null);
    if (full_name !== undefined) add('full_name=$idx', full_name || null);
    if (email !== undefined) {
      // Uniqueness check for email
      if (email) {
        const e = await pool.query('SELECT 1 FROM public.users WHERE email=$1 AND id<>$2', [email, id]);
        if (e.rows.length) return res.status(409).json({ error: 'Email already in use' });
      }
      add('email=$idx', email || null);
    }
    if (phone !== undefined) add('phone=$idx', phone || null);
    if (username !== undefined) {
      if (username) {
        const u = await pool.query('SELECT 1 FROM public.users WHERE LOWER(username)=LOWER($1) AND id<>$2', [username, id]);
        if (u.rows.length) return res.status(409).json({ error: 'Username already in use' });
      }
      add('username=$idx', username || null);
    }
    if (role !== undefined) add('role=$idx', role);

    if (!sets.length) return res.status(400).json({ error: 'No updatable fields provided' });
    params.push(id);
    const sql = `UPDATE public.users SET ${sets.join(', ')} WHERE id=$${params.length} RETURNING id, email, username, phone, full_name, role, active, created_at, last_login, joining_date, status`;
    const r = await pool.query(sql, params);
    if (!r.rows.length) return res.status(404).json({ error: 'User not found' });
    res.json(r.rows[0]);
  } catch (err) {
    console.error(err); res.status(500).json({ error: err.message });
  }
});

// Reset password for a specific user (ADMIN: can reset OWNER/ADMIN/EMPLOYEE; OWNER: can reset OWNER/EMPLOYEE)
app.post('/api/users/:id/password-reset', requireAuth, requireRole('OWNER','ADMIN'), async (req, res) => {
  try {
    const id = req.params.id;
    const { newPassword } = req.body || {};
    if (!newPassword || String(newPassword).length < 6) {
      return res.status(400).json({ error: 'newPassword must be at least 6 characters' });
    }
    const ru = await pool.query('SELECT id, role, active, email, username, full_name FROM public.users WHERE id=$1', [id]);
    if (!ru.rows.length) return res.status(404).json({ error: 'User not found' });
    const target = ru.rows[0];
    if (!target.active) return res.status(400).json({ error: 'User is inactive' });
    // Owner cannot reset Admin passwords
    if (req.user.role === 'OWNER' && target.role === 'ADMIN') {
      return res.status(403).json({ error: 'OWNER cannot reset ADMIN password' });
    }
    const hash = await hashPassword(String(newPassword));
    await pool.query('UPDATE public.users SET password_hash=$1, must_change_password=FALSE, last_password_change_at=NOW() WHERE id=$2', [hash, id]);
    // Audit log (non-fatal on failure)
    try {
      const actorId = req.user && req.user.sub;
      const actorRole = req.user && req.user.role;
      // Username-first policy for audit actor label
      const actor = (req.user && (req.user.username || req.user.full_name || req.user.email)) || getActor(req);
      await pool.query(
        `INSERT INTO public.users_password_audit (
           target_user_id, target_email, target_username, target_full_name, target_role,
           changed_by_user_id, changed_by, changed_by_role, performed_at
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())`,
        [target.id, target.email || null, target.username || null, target.full_name || null, target.role || null,
         actorId || null, actor || null, actorRole || null]
      );
    } catch (e) {
      console.warn('users_password_audit insert failed:', e.message);
    }
    return res.json({ success: true });
  } catch (err) {
    console.error(err); res.status(500).json({ error: err.message });
  }
});

// =============================
// Profile APIs
// =============================
// Get my profile (combines users + user_profiles)
app.get('/api/profile/me', requireAuth, async (req, res) => {
  try {
    const userId = req.user.sub;
    const ur = await pool.query('SELECT id, email, username, phone, full_name, role, joining_date, status FROM public.users WHERE id=$1', [userId]);
    if (!ur.rows.length) return res.status(404).json({ error: 'User not found' });
    const pr = await pool.query('SELECT date_of_birth, gender, emergency_contact_name, emergency_contact_phone, address, pan, aadhaar, aadhaar_last4, updated_at FROM public.user_profiles WHERE user_id=$1', [userId]);
    const base = ur.rows[0];
    const prof = pr.rows[0] || null;
    res.json({ user: base, profile: prof });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// Update my profile
app.put('/api/profile', requireAuth, async (req, res) => {
  try {
    const userId = req.user.sub;
    const { full_name, phone, email, date_of_birth, gender, emergency_contact_name, emergency_contact_phone, address, pan, aadhaar } = req.body || {};
    let panNorm = null;
    if (pan) { if (!isValidPan(pan)) return res.status(400).json({ error: 'Invalid PAN format' }); panNorm = normalizePan(pan); }
    if (aadhaar) { if (!isValidAadhaar(aadhaar)) return res.status(400).json({ error: 'Invalid Aadhaar number' }); }
    // Phone normalization: return 400 if provided but invalid
    let phoneNorm = undefined;
    if (phone !== undefined) {
      if (phone === null || String(phone).trim()==='') phoneNorm = null; else {
        const n = normalizePhone(phone);
        if (!n) return res.status(400).json({ error: 'Invalid phone' });
        phoneNorm = n;
      }
    }

    if (full_name !== undefined || phone !== undefined || email !== undefined) {
      const r = await pool.query('UPDATE public.users SET full_name=COALESCE($1, full_name), phone=COALESCE($2, phone), email=COALESCE($3, email) WHERE id=$4 RETURNING id', [full_name ?? null, phoneNorm ?? null, email ? String(email).toLowerCase() : null, userId]);
      if (!r.rows.length) return res.status(404).json({ error: 'User not found' });
    }

    const existing = await pool.query('SELECT 1 FROM public.user_profiles WHERE user_id=$1', [userId]);
    const aadhaarLast4 = aadhaar ? last4(aadhaar) : undefined;
    if (existing.rows.length) {
      const up = await pool.query(
        `UPDATE public.user_profiles SET date_of_birth=$1, gender=$2, emergency_contact_name=$3, emergency_contact_phone=$4, address=$5, pan=$6, pan_normalized=$7, aadhaar=$8, aadhaar_last4=COALESCE($9, aadhaar_last4) WHERE user_id=$10 RETURNING date_of_birth, gender, emergency_contact_name, emergency_contact_phone, address, pan, aadhaar, aadhaar_last4, updated_at`,
        [date_of_birth || null, gender || null, emergency_contact_name || null, emergency_contact_phone || null, address || null, pan || null, panNorm || null, aadhaar || null, aadhaarLast4 || null, userId]
      );
      return res.json({ ok: true, profile: up.rows[0] });
    } else {
      const ins = await pool.query(
        `INSERT INTO public.user_profiles (user_id, date_of_birth, gender, emergency_contact_name, emergency_contact_phone, address, pan, pan_normalized, aadhaar, aadhaar_last4) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING date_of_birth, gender, emergency_contact_name, emergency_contact_phone, address, pan, aadhaar, aadhaar_last4, updated_at`,
        [userId, date_of_birth || null, gender || null, emergency_contact_name || null, emergency_contact_phone || null, address || null, pan || null, panNorm || null, aadhaar || null, aadhaarLast4 || null]
      );
      return res.json({ ok: true, profile: ins.rows[0] });
    }
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// Upload or replace my photo
app.post('/api/profile/photo', requireAuth, async (req, res) => {
  try {
    const userId = req.user.sub;
    const { dataUrl } = req.body || {};
    if (!dataUrl || typeof dataUrl !== 'string' || !dataUrl.startsWith('data:')) return res.status(400).json({ error: 'dataUrl required' });
    const m = /^data:(.*?);base64,(.*)$/.exec(dataUrl);
    if (!m) return res.status(400).json({ error: 'Invalid dataUrl' });
    const mime = m[1];
    const b64 = m[2];
    const buf = Buffer.from(b64, 'base64');
    if (!/^image\/(png|jpeg|jpg|webp)$/.test(mime)) return res.status(400).json({ error: 'Only PNG/JPEG/WEBP allowed' });
    if (buf.length > 5 * 1024 * 1024) return res.status(400).json({ error: 'Image too large' });

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query('DELETE FROM public.user_photos WHERE user_id=$1', [userId]);
      const ins = await client.query('INSERT INTO public.user_photos (user_id, mime_type, file_name, file_size_bytes, data) VALUES ($1,$2,$3,$4,$5) RETURNING id', [userId, mime, 'profile.' + (mime.split('/')[1] || 'png'), buf.length, buf]);
      await client.query('COMMIT');
      res.json({ ok: true, id: ins.rows[0].id });
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// Get my photo
app.get('/api/profile/photo/me', requireAuth, async (req, res) => {
  try {
    const userId = req.user.sub;
    const r = await pool.query('SELECT mime_type, data FROM public.user_photos WHERE user_id=$1 LIMIT 1', [userId]);
    if (!r.rows.length) return res.status(404).end();
    res.setHeader('Content-Type', r.rows[0].mime_type);
    res.setHeader('Cache-Control', 'private, max-age=300');
    res.send(r.rows[0].data);
  } catch (e) {
    console.error(e);
    res.status(500).end();
  }
});

// Delete my photo
app.delete('/api/profile/photo', requireAuth, async (req, res) => {
  try {
    const userId = req.user.sub;
    await pool.query('DELETE FROM public.user_photos WHERE user_id=$1', [userId]);
    res.json({ ok: true });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// Get a specific user's photo (Admin/Owner only, or self)
app.get('/api/profile/photo/:userId', requireAuth, async (req, res) => {
  try {
    const targetId = req.params.userId;
    if (!(req.user.sub === targetId || req.user.role === 'OWNER' || req.user.role === 'ADMIN')) {
      return res.status(403).end();
    }
    const r = await pool.query('SELECT mime_type, data FROM public.user_photos WHERE user_id=$1 LIMIT 1', [targetId]);
    if (!r.rows.length) return res.status(404).end();
    res.setHeader('Content-Type', r.rows[0].mime_type);
    res.setHeader('Cache-Control', 'private, max-age=300');
    res.send(r.rows[0].data);
  } catch (e) { console.error(e); res.status(500).end(); }
});
// Get user permissions (OWNER, ADMIN, or the user themselves)
app.get('/api/users/:id/permissions', requireAuth, async (req, res) => {
  try {
    const id = req.params.id;
    if (req.user.role === 'OWNER' || req.user.role === 'ADMIN' || req.user.sub === id) {
      // ok
    } else {
      return res.status(403).json({ error: 'Forbidden' });
    }
    const r = await pool.query('SELECT user_id, tabs, actions FROM public.user_permissions WHERE user_id=$1', [id]);
    if (!r.rows.length) return res.json({ user_id: id, tabs: {}, actions: {} });
    res.json(r.rows[0]);
  } catch (err) {
    console.error(err); res.status(500).json({ error: err.message });
  }
});

// Update user permissions (OWNER or ADMIN). Admin can overwrite Owner and Employee permissions.
app.patch('/api/users/:id/permissions', requireAuth, requireRole('OWNER','ADMIN'), async (req, res) => {
  try {
    const id = req.params.id;
    let { tabs, actions, merge } = req.body || {};
    tabs = tabs && typeof tabs === 'object' ? tabs : {};
    actions = actions && typeof actions === 'object' ? actions : {};
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
  const existing = await client.query('SELECT * FROM public.user_permissions WHERE user_id=$1 FOR UPDATE', [id]);
      if (existing.rows.length) {
        if (merge) {
          tabs = { ...existing.rows[0].tabs, ...tabs };
          actions = { ...existing.rows[0].actions, ...actions };
        }
        const upd = await client.query('UPDATE public.user_permissions SET tabs=$1, actions=$2 WHERE user_id=$3 RETURNING user_id, tabs, actions', [tabs, actions, id]);
        await client.query('COMMIT');
        return res.json(upd.rows[0]);
      } else {
        const ins = await client.query('INSERT INTO public.user_permissions (user_id, tabs, actions) VALUES ($1,$2,$3) RETURNING user_id, tabs, actions', [id, tabs, actions]);
        await client.query('COMMIT');
        return res.status(201).json(ins.rows[0]);
      }
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  } catch (err) {
    console.error(err); res.status(500).json({ error: err.message });
  }
});

// Read-only combined user profiles (Admin/Owner only). Data source: public.user_full_profiles view
app.get('/api/admin/employee-profiles', requireAuth, requireRole('OWNER','ADMIN'), async (req, res) => {
  try {
    await ensureUserFullProfilesView(pool);
    let { role, q, page = 1, pageSize = 20 } = req.query || {};
    const p = Math.max(parseInt(page, 10) || 1, 1);
    const s = Math.min(Math.max(parseInt(pageSize, 10) || 20, 1), 100);
    const offset = (p - 1) * s;
    const params = [];
    const filters = [];
    if (role) {
      const allowed = ['OWNER','ADMIN','EMPLOYEE'];
      const roles = String(role).split(',').map(x => x.trim().toUpperCase()).filter(r => allowed.includes(r));
      if (roles.length) {
        const ph = roles.map((_, i) => `$${params.length + i + 1}`).join(',');
        params.push(...roles);
        filters.push(`v.role IN (${ph})`);
      }
    }
    if (q) {
      const like = `%${String(q).toLowerCase()}%`;
      params.push(like, like, like, like);
      const b = params.length;
      filters.push(`(
        LOWER(COALESCE(v.full_name,'')) LIKE $${b-3} OR
        LOWER(COALESCE(v.username,'')) LIKE $${b-2} OR
        LOWER(COALESCE(v.email,'')) LIKE $${b-1} OR
        LOWER(COALESCE(v.phone,'')) LIKE $${b}
      )`);
    }
    const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';
    const order = 'ORDER BY v.role, COALESCE(v.full_name, v.username, v.email)';
    const dataSql = `SELECT v.* FROM public.user_full_profiles v ${where} ${order} LIMIT ${s} OFFSET ${offset}`;
    const countSql = `SELECT COUNT(*)::int AS total FROM public.user_full_profiles v ${where}`;
    const [r, c] = await Promise.all([
      pool.query(dataSql, params),
      pool.query(countSql, params)
    ]);
    res.json({ items: r.rows, page: p, pageSize: s, total: c.rows[0]?.total || 0 });
  } catch (err) {
    console.error(err); res.status(500).json({ error: err.message });
  }
});

// Single employee profile by id (Admin/Owner only)
app.get('/api/admin/employee-profile/:userId', requireAuth, requireRole('OWNER','ADMIN'), async (req, res) => {
  try {
    await ensureUserFullProfilesView(pool);
    const id = req.params.userId;
    const r = await pool.query('SELECT * FROM public.user_full_profiles WHERE user_id=$1', [id]);
    if (!r.rows.length) return res.status(404).json({ error: 'Not found' });
    res.json(r.rows[0]);
  } catch (err) { console.error(err); res.status(500).json({ error: err.message }); }
});



// ===========================
// Expense Audit Endpoint
// ===========================
app.get('/api/expenses-audit', async (req, res) => {
  const { opportunityId, client, q, action, dateFrom, dateTo, page = 1, pageSize = 50, sort } = req.query;
  const p = Math.max(Number(page) || 1, 1);
  const s = Math.min(Math.max(Number(pageSize) || 50, 1), 500);
  const offset = (p - 1) * s;
  const params = [];
  const filters = [];
  if (opportunityId) { params.push(opportunityId); filters.push(`ea.opportunity_id = $${params.length}`); }
  if (client) { params.push(`%${String(client).toLowerCase()}%`); filters.push(`LOWER(o.client_name) LIKE $${params.length}`); }
  if (q) {
    const like = `%${String(q).toLowerCase()}%`;
    params.push(like); // opp id
    params.push(like); // client name
    filters.push(`(LOWER(ea.opportunity_id) LIKE $${params.length-1} OR LOWER(o.client_name) LIKE $${params.length})`);
  }
  if (action) {
    const actions = String(action).split(',').map(x => x.trim().toUpperCase()).filter(Boolean);
    if (actions.length) {
      const ph = actions.map((_, i) => `$${params.length + i + 1}`).join(',');
      params.push(...actions);
      filters.push(`action IN (${ph})`);
    }
  }
  if (dateFrom) { params.push(dateFrom); filters.push(`ea.performed_at >= $${params.length}`); }
  if (dateTo) { params.push(dateTo); filters.push(`ea.performed_at <= $${params.length}`); }
  const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';
  const order = (String(sort||'').toLowerCase() === 'performed_at_asc') ? 'ORDER BY ea.performed_at ASC' : 'ORDER BY ea.performed_at DESC';
  try {
    const sql = `
      SELECT ea.opportunity_id, o.client_name, ea.action, ea.old_amount, ea.new_amount, ea.old_note, ea.new_note, ea.performed_by, ea.performed_at
      FROM expenses_audit ea
      LEFT JOIN opportunities o ON o.opportunity_id = ea.opportunity_id
      ${where}
      ${order}
      LIMIT ${s} OFFSET ${offset}
    `;
    const rows = await pool.query(sql, params);
    res.json({ items: rows.rows, page: p, pageSize: s });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// ===========================
// Password Audit Endpoint
// ===========================
app.get('/api/password-audit', requireAuth, requireRole('OWNER','ADMIN'), async (req, res) => {
  try {
    const { target, actor, dateFrom, dateTo, page = 1, pageSize = 50 } = req.query;
    const p = Math.max(Number(page) || 1, 1);
    const s = Math.min(Math.max(Number(pageSize) || 50, 1), 500);
    const offset = (p - 1) * s;
    const params = [];
    const filters = [];
    if (target) {
      // match against email/username/full_name
      const like = `%${String(target).toLowerCase()}%`;
      params.push(like, like, like);
      filters.push('(LOWER(COALESCE(target_email,\'\')) LIKE $'+(params.length-2)+' OR LOWER(COALESCE(target_username,\'\')) LIKE $'+(params.length-1)+' OR LOWER(COALESCE(target_full_name,\'\')) LIKE $'+(params.length)+')');
    }
    if (actor) {
      const like = `%${String(actor).toLowerCase()}%`;
      params.push(like);
      filters.push('(LOWER(COALESCE(changed_by,\'\')) LIKE $'+(params.length)+')');
    }
    if (dateFrom) { params.push(dateFrom); filters.push('performed_at >= $'+(params.length)); }
    if (dateTo) { params.push(dateTo); filters.push('performed_at <= $'+(params.length)); }
    const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';
    const sql = `
      SELECT id, target_email, target_username, target_full_name, target_role,
             changed_by, changed_by_role, performed_at
        FROM public.users_password_audit
        ${where}
        ORDER BY performed_at DESC
        LIMIT ${s} OFFSET ${offset}
    `;
    const r = await pool.query(sql, params);
    res.json({ items: r.rows, page: p, pageSize: s });
  } catch (err) {
    console.error(err); res.status(500).json({ error: err.message });
  }
});

// ===========================
// Stage Control Endpoints
// ===========================

// Allowed transitions helper (shared)
function getAllowedTransitions(fromStage) {
  // Non-linear: allow any-to-any except same-stage
  const ALL = ['LEAD','QUALIFIED','NEGOTIATION','AGREED','DISAGREED','CANCELLED'];
  return ALL.filter(s => s !== (fromStage || 'LEAD'));
}
function transitionRequiresReason(fromStage, toStage) {
  if (toStage === 'DISAGREED' || toStage === 'CANCELLED') return true;
  if ((fromStage === 'DISAGREED' || fromStage === 'CANCELLED') && ['LEAD','QUALIFIED','NEGOTIATION','AGREED'].includes(toStage)) return true;
  return false;
}

// GET stage history
app.get('/api/opportunities/:id/stage-history', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM opportunity_stage_audit WHERE opportunity_id = $1 ORDER BY changed_at DESC', [req.params.id]);
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// GET allowed transitions for current stage
app.get('/api/opportunities/:id/allowed-transitions', async (req, res) => {
  try {
    const r = await pool.query('SELECT stage FROM opportunities WHERE opportunity_id = $1', [req.params.id]);
    if (r.rows.length === 0) return res.status(404).json({ error: 'Not found' });
    const current = r.rows[0].stage || 'LEAD';
    const transitions = getAllowedTransitions(current).map(to => ({ toStage: to, reasonRequired: transitionRequiresReason(current, to) }));
    res.json({ currentStage: current, transitions });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// POST stage change (strict validator path for future UI)
app.post('/api/opportunities/:id/stage', requireAuth, async (req, res) => {
  const { toStage, reasonCode, reasonText } = req.body || {};
  if (!toStage) return res.status(400).json({ error: 'toStage is required' });
  const oppId = req.params.id;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const r = await client.query('SELECT * FROM opportunities WHERE opportunity_id = $1 FOR UPDATE', [oppId]);
    if (r.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Not found' });
    }
    const current = r.rows[0];
    const fromStage = current.stage || 'LEAD';
    if (fromStage === toStage) {
      await client.query('ROLLBACK');
      return res.json(current);
    }
    const allowed = getAllowedTransitions(fromStage);
    if (!allowed.includes(toStage)) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: `Invalid transition ${fromStage} -> ${toStage}` });
    }
    const needReason = transitionRequiresReason(fromStage, toStage);
    if (needReason && !reasonCode) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'reasonCode is required for this transition' });
    }

    const actor = getActor(req);
    await client.query(
      'INSERT INTO opportunity_stage_audit (opportunity_id, from_stage, to_stage, reason_code, reason_text, changed_by, source) VALUES ($1,$2,$3,$4,$5,$6,$7)',
      [oppId, fromStage, toStage, reasonCode || null, reasonText || null, actor, 'user']
    );
    const upd = await client.query('UPDATE opportunities SET stage=$1 WHERE opportunity_id=$2 RETURNING *', [toStage, oppId]);
    const updatedOpp = upd.rows[0];

    // Side effects similar to PUT
    if (toStage === 'AGREED') {
      if (current.assignment === 'CUSTOMER') {
        const existing = await client.query('SELECT * FROM customers WHERE opportunity_id = $1', [oppId]);
        if (existing.rows.length === 0) {
          const customer_id = generateCustomerId();
          await client.query('INSERT INTO customers (customer_id, opportunity_id, client_name, created_at) VALUES ($1,$2,$3,NOW())', [customer_id, oppId, current.client_name]);
          await client.query('INSERT INTO customer_status_audit (customer_id, from_status, to_status, source) VALUES ($1,$2,$3,$4)', [customer_id, 'ACTIVE', 'ACTIVE', 'system']);
        } else {
          // Reactivate any non-active customers on reopen to AGREED
          for (const row of existing.rows) {
            if (row.customer_status && row.customer_status !== 'ACTIVE') {
              await client.query('UPDATE customers SET customer_status=$1 WHERE customer_id=$2', ['ACTIVE', row.customer_id]);
              await client.query('INSERT INTO customer_status_audit (customer_id, from_status, to_status, reason_code, reason_text, changed_by, source) VALUES ($1,$2,$3,$4,$5,$6,$7)', [row.customer_id, row.customer_status, 'ACTIVE', 'reopen', null, actor, 'user']);
            }
          }
        }
      }
      if (current.assignment === 'CONTRACT') {
        const existingContract = await client.query('SELECT * FROM contracts WHERE opportunity_id = $1', [oppId]);
        if (existingContract.rows.length === 0) {
          const contract_id = Math.random().toString(36).substr(2, 6).toUpperCase();
          await client.query(
            `INSERT INTO contracts (
              contract_id, opportunity_id, client_name, quoted_price_per_litre, start_date, end_date, credit_period, primary_contact, phone_number, alt_phone, gstin, email, created_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW())`,
            [contract_id, oppId, current.client_name, current.proposed_price_per_litre, null, null, null, null, null, null, null, null]
          );
          await client.query('INSERT INTO contract_status_audit (contract_id, from_status, to_status, source) VALUES ($1,$2,$3,$4)', [contract_id, 'ACTIVE', 'ACTIVE', 'system']);
        } else {
          // Reactivate any cancelled contracts when reopening to AGREED
          for (const row of existingContract.rows) {
            if (row.contract_status === 'CANCELLED') {
              await client.query('UPDATE contracts SET contract_status=$1 WHERE contract_id=$2', ['ACTIVE', row.contract_id]);
              await client.query('INSERT INTO contract_status_audit (contract_id, from_status, to_status, reason_code, reason_text, changed_by, source) VALUES ($1,$2,$3,$4,$5,$6,$7)', [row.contract_id, 'CANCELLED', 'ACTIVE', 'reopen', null, actor, 'user']);
            }
          }
        }
      }
    }
    if (fromStage === 'AGREED' && toStage === 'CANCELLED') {
      const contractsRes = await client.query('SELECT contract_id FROM contracts WHERE opportunity_id = $1', [oppId]);
      for (const row of contractsRes.rows) {
        await client.query('UPDATE contracts SET contract_status=$1 WHERE contract_id=$2', ['CANCELLED', row.contract_id]);
        await client.query('INSERT INTO contract_status_audit (contract_id, from_status, to_status, reason_code, reason_text, changed_by, source) VALUES ($1,$2,$3,$4,$5,$6,$7)', [row.contract_id, 'ACTIVE', 'CANCELLED', reasonCode || null, reasonText || null, actor, 'user']);
      }
      // Cancel related customers
      const customersRes = await client.query('SELECT customer_id, customer_status FROM customers WHERE opportunity_id = $1', [oppId]);
      for (const row of customersRes.rows) {
        if (row.customer_status !== 'CANCELLED') {
          await client.query('UPDATE customers SET customer_status=$1 WHERE customer_id=$2', ['CANCELLED', row.customer_id]);
          await client.query('INSERT INTO customer_status_audit (customer_id, from_status, to_status, reason_code, reason_text, changed_by, source) VALUES ($1,$2,$3,$4,$5,$6,$7)', [row.customer_id, row.customer_status || 'ACTIVE', 'CANCELLED', reasonCode || null, reasonText || null, actor, 'user']);
        }
      }
    }

    await client.query('COMMIT');
    res.json(updatedOpp);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error(err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// Aggregated history endpoint
app.get('/api/history', async (req, res) => {
  const { opportunityId, client, q, entityType, dateFrom, dateTo, page = 1, pageSize = 50 } = req.query;
  const p = Number(page) || 1;
  const s = Math.min(Number(pageSize) || 50, 500);
  const offset = (p - 1) * s;
  try {
    // Build filters
    const params = [];
    const filters = [];
    if (opportunityId) { params.push(opportunityId); filters.push(`(ah.opportunity_id = $${params.length})`); }
    if (client) { params.push(`%${client.toLowerCase()}%`); filters.push(`(LOWER(ah.client_name) LIKE $${params.length})`); }
    if (q) {
      const like = `%${String(q).toLowerCase()}%`;
      params.push(like); // opp
      params.push(like); // client
      params.push(like); // entity id (customer/contract/opportunity)
      filters.push(`(
        LOWER(ah.opportunity_id) LIKE $${params.length-2}
        OR LOWER(ah.client_name) LIKE $${params.length-1}
        OR LOWER(ah.entity_id) LIKE $${params.length}
      )`);
    }
    if (entityType) {
      const list = String(entityType).split(',').map(s => s.trim()).filter(Boolean);
      if (list.length) {
        const placeholders = list.map((_, i) => `$${params.length + i + 1}`).join(',');
        params.push(...list);
        filters.push(`(ah.entity_type IN (${placeholders}))`);
      }
    }
    if (dateFrom) { params.push(dateFrom); filters.push(`(ah.changed_at >= $${params.length})`); }
    if (dateTo) { params.push(dateTo); filters.push(`(ah.changed_at <= $${params.length})`); }

    const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';

    const sql = `
      WITH opp AS (
   SELECT oa.id, 'opportunity' AS entity_type, oa.opportunity_id AS entity_id, oa.opportunity_id, o.client_name,
     oa.from_stage AS from_value, oa.to_stage AS to_value, oa.reason_code, oa.reason_text,
     COALESCE(oa.changed_by, oa.source) AS changed_by, oa.changed_at
        FROM opportunity_stage_audit oa
        JOIN opportunities o ON o.opportunity_id = oa.opportunity_id
      ),
      con AS (
   SELECT ca.id, 'contract' AS entity_type, ca.contract_id AS entity_id, c.opportunity_id, c.client_name,
     ca.from_status AS from_value, ca.to_status AS to_value, ca.reason_code, ca.reason_text,
     COALESCE(ca.changed_by, ca.source) AS changed_by, ca.changed_at
        FROM contract_status_audit ca
        JOIN contracts c ON c.contract_id = ca.contract_id
      ),
      cus AS (
   SELECT cua.id, 'customer' AS entity_type, cua.customer_id AS entity_id, cu.opportunity_id, cu.client_name,
     cua.from_status AS from_value, cua.to_status AS to_value, cua.reason_code, cua.reason_text,
     COALESCE(cua.changed_by, cua.source) AS changed_by, cua.changed_at
        FROM customer_status_audit cua
        JOIN customers cu ON cu.customer_id = cua.customer_id
      ),
      all_hist AS (
        SELECT * FROM opp
        UNION ALL
        SELECT * FROM con
        UNION ALL
        SELECT * FROM cus
      )
      SELECT ah.id, ah.entity_type, ah.entity_id, ah.opportunity_id, ah.client_name,
             ah.from_value, ah.to_value, ah.reason_code, ah.reason_text, ah.changed_by, ah.changed_at
      FROM all_hist ah
      ${where}
      ORDER BY ah.changed_at DESC
      LIMIT ${s} OFFSET ${offset}
    `;
    const rows = await pool.query(sql, params);

    // Note: For total count we can run a COUNT(*) over the same CTE without LIMIT for pagination UI if needed later
    res.json({ items: rows.rows, page: p, pageSize: s });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

