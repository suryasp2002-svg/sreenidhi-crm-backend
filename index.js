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
const { sendEmail } = require('./utils/mailer');
const { generateICS, generateGoogleCalendarLink } = require('./utils/calendar');
const { meetingEmailHtml } = require('./utils/templates/meetingEmail');
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
    // Ensure pgcrypto for gen_random_uuid (needed by users.id default)
    try {
      await db.query("CREATE EXTENSION IF NOT EXISTS pgcrypto");
    } catch (e) {
      // Non-fatal on providers that restrict CREATE EXTENSION
      if (!process.env.SUPPRESS_DB_LOG) console.warn('[ensureMinimalSchema] pgcrypto warn:', e.message);
    }

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

    // Minimal users table to unblock auth if migrations haven't run yet
    await db.query(`
      CREATE TABLE IF NOT EXISTS public.users (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
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
    // Prefer email, then username for canonical comparisons
    return req.user.email || req.user.username || req.user.sub || 'user';
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
  return u.email || u.username || u.full_name || null;
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
  // Accept a parseable ISO as a fallback
  const d = new Date(s);
  return !isNaN(d.getTime());
}
async function validateSelectableUserId(db, userId) {
  if (!userId) return null;
  // We consider OWNER or EMPLOYEE selectable; active only
  const r = await db.query(`SELECT id FROM public.users WHERE id=$1 AND active=TRUE AND role IN ('OWNER','EMPLOYEE')`, [userId]);
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
      assignedLabel = rr.rows.length ? (rr.rows[0].email || rr.rows[0].username || rr.rows[0].full_name) : actor;
    } else {
      if (assignedToUserId) {
        const valid = await validateSelectableUserId(pool, String(assignedToUserId));
        if (!valid) return res.status(400).json({ error: 'Invalid assignedToUserId' });
        assignedUserId = valid;
        const rr = await pool.query('SELECT email, username, full_name FROM public.users WHERE id=$1', [valid]);
        assignedLabel = rr.rows.length ? (rr.rows[0].email || rr.rows[0].username || rr.rows[0].full_name) : null;
      } else if (assigned_to) {
        const u = await resolveUserByIdentifier(assigned_to);
        if (u) { assignedUserId = u.id; assignedLabel = pickDisplay(u); }
      }
      if (!assignedUserId) {
        assignedUserId = actorUserId;
        const rr = await pool.query('SELECT email, username, full_name FROM public.users WHERE id=$1', [actorUserId]);
        assignedLabel = rr.rows.length ? (rr.rows[0].email || rr.rows[0].username || rr.rows[0].full_name) : actor;
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
      assignedLabel = rr.rows.length ? (rr.rows[0].email || rr.rows[0].username || rr.rows[0].full_name) : actor;
    } else {
      if (assignedToUserId) {
        const valid = await validateSelectableUserId(pool, String(assignedToUserId));
        if (!valid) return res.status(400).json({ error: 'Invalid assignedToUserId' });
        assignedUserId = valid;
        const rr = await pool.query('SELECT email, username, full_name FROM public.users WHERE id=$1', [valid]);
        assignedLabel = rr.rows.length ? (rr.rows[0].email || rr.rows[0].username || rr.rows[0].full_name) : null;
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
function normalizeTimestamp(ts) {
  if (!ts) return null;
  const d = new Date(ts);
  if (isNaN(d.getTime())) return null;
  return d.toISOString().slice(0,19).replace('T',' ');
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
             uc.email AS created_by_email
      FROM meetings m
      LEFT JOIN customers c ON c.customer_id = m.customer_id
      LEFT JOIN opportunities o ON o.opportunity_id = m.opportunity_id
      LEFT JOIN public.users ua ON ua.id = m.assigned_to_user_id
      LEFT JOIN public.users uc ON uc.id = m.created_by_user_id
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
      SELECT m.id, m.subject, m.starts_at, m.location, m.person_name,
             COALESCE(c.client_name, o.client_name) AS client_name
        FROM meetings m
        LEFT JOIN customers c ON c.customer_id = m.customer_id
        LEFT JOIN opportunities o ON o.opportunity_id = m.opportunity_id
       WHERE m.id = $1
       LIMIT 1
    `, [id]);
    if (!r.rows.length) return res.status(404).send('Not found');
    const row = r.rows[0];
    const dto = buildMeetingDto({ id: row.id, subject: row.subject, clientName: row.client_name, personName: row.person_name, startsAt: row.starts_at, endsAt: null, location: row.location });
    const ics = await generateICS(dto);
    res.setHeader('Content-Type', 'text/calendar; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="meeting-${encodeURIComponent(id)}.ics"`);
    return res.send(ics);
  } catch (e) {
    console.error(e);
    res.status(500).send('Server error');
  }
});

// Create meeting with audit
app.post('/api/meetings', requireAuth, async (req, res) => {
  let { id, customer_id, opportunity_id, contract_id, subject, starts_at, when_ts, location, person_name, contact_phone, notes, status, assigned_to, assignedToUserId } = req.body || {};
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
         assigned_to, assigned_to_user_id, created_by, created_by_user_id, created_at, updated_at)
       VALUES (
         $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
         $13,$14,$15,$16,NOW(),NOW()
       ) RETURNING *`,
      [id, customer_id, opportunity_id || null, contract_id || null, subject, start, start, location || null, person_name, contact_phone, notes || null, st,
       assignedLabel || null, assignedUserId || null, actor, actorUserId]
    );
    const row = ins.rows[0];
    await client.query(
      `INSERT INTO meetings_audit (meeting_id, action, performed_by, before_subject, after_subject, before_starts_at, after_starts_at, before_status, after_status, outcome_notes)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
      [row.id, 'CREATE', actor, null, row.subject, null, row.starts_at, null, row.status, null]
    );
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
  let { customer_id, opportunity_id, contract_id, subject, starts_at, when_ts, location, person_name, contact_phone, notes, status, assigned_to, assignedToUserId } = req.body || {};
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
      `UPDATE meetings SET customer_id=$1, opportunity_id=$2, contract_id=$3, subject=$4, starts_at=$5, when_ts=$6, location=$7, person_name=$8, contact_phone=$9, notes=$10, status=$11, assigned_to=$12, assigned_to_user_id=$13, updated_at=NOW()
       WHERE id=$14 RETURNING *`,
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
    const sql = `
      SELECT r.*,
             uc.full_name AS created_by_full_name,
             uc.username AS created_by_username,
             uc.email AS created_by_email
      FROM reminders r
      LEFT JOIN public.users uc ON uc.id = r.created_by_user_id
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
      assigneeLabel = rr.rows.length ? (rr.rows[0].email || rr.rows[0].username || rr.rows[0].full_name) : getActor(req);
    }
    if (assignedToUserId) {
      // Allow self-assignment for any role, including ADMIN
      if (req.user && String(assignedToUserId) === String(req.user.sub)) {
        assigneeUserId = req.user.sub;
        const rr = await pool.query('SELECT email, username, full_name FROM public.users WHERE id=$1', [assigneeUserId]);
        assigneeLabel = rr.rows.length ? (rr.rows[0].email || rr.rows[0].username || rr.rows[0].full_name) : getActor(req);
      } else {
        const valid = await validateSelectableUserId(pool, String(assignedToUserId));
        if (!valid) return res.status(400).json({ error: 'Invalid assignedToUserId' });
        assigneeUserId = valid;
        const rr = await pool.query('SELECT email, username, full_name FROM public.users WHERE id=$1', [valid]);
        assigneeLabel = rr.rows.length ? (rr.rows[0].email || rr.rows[0].username || rr.rows[0].full_name) : null;
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
        SELECT m.id, m.subject, m.starts_at, m.location, m.person_name,
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
  meeting = buildMeetingDto({ id: row.id, subject: row.subject, clientName: row.client_name, personName: row.person_name, startsAt: row.starts_at, endsAt: null, location: row.location });
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
      meeting = buildMeetingDto({ id: b.id || 'preview', subject: b.title, clientName: b.clientName, personName: b.personName, startsAt: b.startsAt, endsAt: b.endsAt, location: b.location, meetingLink: b.meetingLink });
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

// Send an email (generic) with optional ICS attachment generated from meetingId or meeting payload
// Body: { to, cc?, bcc?, subject, html, meetingId?, meeting? }
app.post('/api/email/send', requireAuth, async (req, res) => {
  try {
    let { to, cc, bcc, subject, html, meetingId, meeting } = req.body || {};
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
          SELECT m.id, m.subject, m.starts_at, m.location, m.person_name,
                 COALESCE(c.client_name, o.client_name) AS client_name
            FROM meetings m
            LEFT JOIN customers c ON c.customer_id = m.customer_id
            LEFT JOIN opportunities o ON o.opportunity_id = m.opportunity_id
           WHERE m.id = $1
           LIMIT 1
        `, [String(meetingId)]);
        if (r.rows.length) {
          const row = r.rows[0];
          dto = buildMeetingDto({ id: row.id, subject: row.subject, clientName: row.client_name, personName: row.person_name, startsAt: row.starts_at, endsAt: null, location: row.location });
        }
      } else {
        dto = buildMeetingDto({ id: meeting.id || 'meeting', subject: meeting.title, clientName: meeting.clientName, personName: meeting.personName, startsAt: meeting.startsAt, endsAt: meeting.endsAt, location: meeting.location, meetingLink: meeting.meetingLink });
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

    try {
      const info = await sendEmail({ to: toList, cc: ccList, bcc: bccList, subject, html, attachments });
      return res.json({ ok: true, messageId: info.messageId });
    } catch (e) {
      // Provide a user-friendly error for missing SMTP config
      const msg = e && e.code === 'SMTP_CONFIG_MISSING' ? e.message : (e.message || 'Email send failed');
      return res.status(500).json({ error: msg });
    }
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
    let { title, due_ts, notes, status, receiver_email, recipient_email, person_name, phone, notify_at } = req.body || {};
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
    // Build update
    const updated = await pool.query(
      `UPDATE reminders
       SET title=$1, due_ts=$2, notes=$3, status=$4, notify_at=$5, receiver_email=$6, person_name=$7, phone=$8
       WHERE id=$9 RETURNING *`,
      [title, dueDate ? normalizeLocal(dueDate) : row.due_ts, notes, status, notifyAtDate ? normalizeLocal(notifyAtDate) : row.notify_at, receiver_email || null, person_name, phone || null, id]
    );
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
    if (req.user && req.user.role === 'EMPLOYEE') {
      const cur = await pool.query('SELECT created_by, created_by_user_id FROM reminders WHERE id=$1', [id]);
      if (!cur.rows.length) return res.status(404).json({ error: 'Not found' });
      const actor = getActor(req);
      if (cur.rows[0].created_by_user_id !== req.user.sub && String(cur.rows[0].created_by || '') !== String(actor)) {
        return res.status(403).json({ error: 'Employees can update only reminders they created' });
      }
    }
    const r = await pool.query('UPDATE reminders SET status=$1 WHERE id=$2 RETURNING *', [String(status).toUpperCase(), id]);
    if (r.rows.length === 0) return res.status(404).json({ error: 'Not found' });
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
    const r = await pool.query(
      'INSERT INTO public.users (email, full_name, role, password_hash, must_change_password) VALUES ($1,$2,$3,$4,$5) RETURNING id, email, full_name, role, created_at',
      [String(email).toLowerCase(), full_name || null, 'OWNER', pwHash, false]
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
    // Look up by username (case-insensitive) or email (case-insensitive)
    const val = String(idOrEmail).trim();
    let r;
    if (val.includes('@')) {
      r = await pool.query('SELECT * FROM public.users WHERE email=$1 AND active=TRUE', [val.toLowerCase()]);
    } else {
      r = await pool.query('SELECT * FROM public.users WHERE LOWER(username)=LOWER($1) AND active=TRUE', [val]);
      // Fallback: if not found and identifier contains spaces, attempt full_name exact (case-insensitive) match when unique
      if (r.rows.length === 0 && /\s/.test(val)) {
        const rf = await pool.query('SELECT * FROM public.users WHERE active=TRUE AND LOWER(full_name)=LOWER($1)', [val]);
        if (rf.rows.length === 1) {
          r = rf;
        }
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
    if ((!email && !username) || !password || !role) return res.status(400).json({ error: 'username/email, password, role required' });
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
    const ins = await pool.query(
      'INSERT INTO public.users (email, username, phone, full_name, role, password_hash, must_change_password, joining_date, status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id, email, username, phone, full_name, role, created_at, joining_date, status',
      [email ? String(email).toLowerCase() : null, username || null, phone || null, full_name || null, rRole, pwHash, true, joining_date || null, status || 'ACTIVE']
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
      const actor = (req.user && (req.user.email || req.user.username)) || getActor(req);
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

