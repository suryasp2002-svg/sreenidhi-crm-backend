const pool = require('../db');

function isoDateOnly(val) {
  if (!val) return null;
  try {
    const d = (val instanceof Date) ? val : new Date(String(val));
    if (isNaN(d.getTime())) return null;
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${y}-${m}-${day}`;
  } catch { return null; }
}

function getActorLabel(req) {
  if (req && req.user) return req.user.username || req.user.email || req.user.sub || 'user';
  return 'user';
}

function getIp(req) {
  if (!req) return null;
  const xf = req.headers && (req.headers['x-forwarded-for'] || req.headers['X-Forwarded-For']);
  if (xf) return Array.isArray(xf) ? xf[0] : String(xf).split(',')[0].trim();
  return req.ip || req.connection?.remoteAddress || null;
}

// Format as local SQL timestamp 'YYYY-MM-DD HH:mm:ss' without timezone.
function fmtSqlLocalTs(input) {
  if (!input) return null;
  // Already in local SQL without TZ
  if (typeof input === 'string') {
    const s = input.trim();
    const plain = s.replace('T', ' ');
    if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}(:\d{2})?$/.test(plain)) {
      return plain.length === 16 ? `${plain}:00` : plain.slice(0,19);
    }
    // If string has timezone (Z or +/-), parse and format to local
    if (/[Zz]|[+-]\d{2}:?\d{2}$/.test(s)) {
      const d = new Date(s);
      if (!isNaN(d.getTime())) {
        const pad = n => String(n).padStart(2,'0');
        return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
      }
    }
    // Fallback: try Date parse and format
    const d2 = new Date(s);
    if (!isNaN(d2.getTime())) {
      const pad = n => String(n).padStart(2,'0');
      return `${d2.getFullYear()}-${pad(d2.getMonth()+1)}-${pad(d2.getDate())} ${pad(d2.getHours())}:${pad(d2.getMinutes())}:${pad(d2.getSeconds())}`;
    }
    return null;
  }
  // Date object
  if (input instanceof Date) {
    const d = input;
    if (isNaN(d.getTime())) return null;
    const pad = n => String(n).padStart(2,'0');
    return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  }
  return null;
}

// Insert a row into fuel_ops_audit. Accepts optional client for transactions.
// entry: {
//  tab, section, action, entity_type, entity_id,
//  unit_id, unit_type, driver_id,
//  op_date, performed_time, amount_liters, meter_reading,
//  payload_old, payload_new, reason, request_id
// }
async function logAudit(req, entry = {}, client) {
  const cx = client || pool;
  const username = getActorLabel(req);
  let userId = null;
  if (req && req.user && req.user.sub != null) {
    const maybe = parseInt(req.user.sub, 10);
    if (Number.isFinite(maybe)) userId = maybe; // only store numeric IDs
  }
  const ip = getIp(req);
  const cols = [
    'tab','section','action','entity_type','entity_id',
    'unit_id','unit_type','driver_id',
    'op_date','performed_time','amount_liters','meter_reading',
    'payload_old','payload_new','reason','request_id',
    'user_id','username','ip_addr'
  ];
  const values = [
    entry.tab || null,
    entry.section || null,
    entry.action || null,
    entry.entity_type || null,
    entry.entity_id != null ? entry.entity_id : null,
    entry.unit_id != null ? entry.unit_id : null,
    entry.unit_type || null,
    entry.driver_id != null ? entry.driver_id : null,
    entry.op_date ? isoDateOnly(entry.op_date) : null,
    (entry.performed_time ? fmtSqlLocalTs(entry.performed_time) : null),
    entry.amount_liters != null ? Number(entry.amount_liters) : null,
    entry.meter_reading != null ? Number(entry.meter_reading) : null,
    entry.payload_old != null ? entry.payload_old : null,
    entry.payload_new != null ? entry.payload_new : null,
    entry.reason || null,
    entry.request_id || null,
    userId,
    username,
    ip
  ];
  try {
    const params = values.map((_, i) => `$${i + 1}`).join(', ');
    await cx.query(
      `INSERT INTO public.fuel_ops_audit (${cols.join(',')}) VALUES (${params})`,
      values
    );
    if (process.env.AUDIT_DEBUG) {
      console.log('[audit inserted]', { tab: entry.tab, action: entry.action, entity: entry.entity_type, id: entry.entity_id });
    }
  } catch (e) {
    if (!process.env.SUPPRESS_DB_LOG) console.warn('[audit warn]', e.message);
    if (process.env.AUDIT_DEBUG) console.warn('[audit debug failed entry]', entry);
  }
}

module.exports = { logAudit, isoDateOnly };
