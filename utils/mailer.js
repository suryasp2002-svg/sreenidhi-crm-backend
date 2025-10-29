const nodemailer = require('nodemailer');

function getBool(v, def = false) {
  if (v === undefined || v === null) return def;
  const s = String(v).toLowerCase();
  return s === '1' || s === 'true' || s === 'yes' || s === 'on';
}

function buildTransportConfig() {
  let host = process.env.SMTP_HOST;
  let port = Number(process.env.SMTP_PORT || 587);
  let secure = getBool(process.env.SMTP_SECURE, false);
  const user = process.env.SMTP_USER;
  const pass = process.env.SMTP_PASS;
  // Convenience: if host unset but user looks like Gmail, default to Gmail settings
  if (!host && user && /@gmail\.com$/i.test(String(user))) {
    host = 'smtp.gmail.com';
    port = 465;
    secure = true;
  }
  // Optional TLS knobs for tricky providers
  const requireTLS = getBool(process.env.SMTP_REQUIRE_TLS, false);
  const ignoreTLS = getBool(process.env.SMTP_IGNORE_TLS, false);
  const rejectUnauthorized = process.env.SMTP_TLS_REJECT_UNAUTHORIZED === undefined
    ? true
    : getBool(process.env.SMTP_TLS_REJECT_UNAUTHORIZED, true);
  // Conservative timeouts to ensure HTTP request doesn't hang forever
  const connectionTimeout = Number(process.env.SMTP_CONNECTION_TIMEOUT_MS || 20000); // 20s
  const socketTimeout = Number(process.env.SMTP_SOCKET_TIMEOUT_MS || 25000); // 25s
  const greetingTimeout = Number(process.env.SMTP_GREETING_TIMEOUT_MS || 15000); // 15s

  const cfg = { host, port, secure, auth: { user, pass }, connectionTimeout, socketTimeout, greetingTimeout };
  if (requireTLS) cfg.requireTLS = true;
  if (ignoreTLS) cfg.ignoreTLS = true;
  cfg.tls = { ...(cfg.tls || {}), rejectUnauthorized };
  return cfg;
}

function validateSmtpConfig(cfg) {
  const missing = [];
  if (!cfg.host) missing.push('SMTP_HOST');
  if (!cfg.auth || !cfg.auth.user) missing.push('SMTP_USER');
  if (!cfg.auth || !cfg.auth.pass) missing.push('SMTP_PASS');
  return missing;
}

function createTransporter() {
  const cfg = buildTransportConfig();
  const missing = validateSmtpConfig(cfg);
  if (missing.length) {
    const hint = `SMTP not configured: missing ${missing.join(', ')}. For Gmail set SMTP_HOST=smtp.gmail.com, SMTP_PORT=465, SMTP_SECURE=true, SMTP_USER, SMTP_PASS (App Password).`;
    const err = new Error(hint);
    err.code = 'SMTP_CONFIG_MISSING';
    throw err;
  }
  return nodemailer.createTransport(cfg);
}

function parseFromAddress(str) {
  const s = String(str || '').trim();
  if (!s) return { email: null, name: null, raw: null };
  const m = s.match(/^(.*)<([^>]+)>$/);
  if (m) {
    const name = m[1].trim().replace(/^"|"$/g, '') || null;
    const email = m[2].trim();
    return { email, name, raw: s };
  }
  return { email: s, name: null, raw: s };
}

async function sendViaSmtp({ to, cc, bcc, subject, html, attachments }) {
  const transporter = createTransporter();
  return await transporter.sendMail({
    from: process.env.MAIL_FROM || process.env.SMTP_USER,
    to: Array.isArray(to) ? to.join(',') : to,
    cc: cc && cc.length ? (Array.isArray(cc) ? cc.join(',') : cc) : undefined,
    bcc: bcc && bcc.length ? (Array.isArray(bcc) ? bcc.join(',') : bcc) : undefined,
    subject,
    html,
    attachments,
  });
}

async function sendViaSendGrid({ to, cc, bcc, subject, html, attachments }) {
  const key = process.env.SENDGRID_API_KEY;
  if (!key) throw new Error('SENDGRID_API_KEY not set');
  const fromRaw = process.env.MAIL_FROM || process.env.SMTP_USER;
  const from = parseFromAddress(fromRaw);
  const toList = (Array.isArray(to) ? to : String(to).split(',')).filter(Boolean).map(e => ({ email: String(e).trim() }));
  const ccList = (cc && (Array.isArray(cc) ? cc : String(cc).split(','))).filter(Boolean).map(e => ({ email: String(e).trim() }));
  const bccList = (bcc && (Array.isArray(bcc) ? bcc : String(bcc).split(','))).filter(Boolean).map(e => ({ email: String(e).trim() }));
  const body = {
    personalizations: [{ to: toList, ...(ccList.length ? { cc: ccList } : {}), ...(bccList.length ? { bcc: bccList } : {}) }],
    from: from.name ? { email: from.email, name: from.name } : { email: from.email },
    subject,
    content: [{ type: 'text/html', value: html || '' }],
  };
  if (getBool(process.env.SENDGRID_SANDBOX, false)) {
    body.mail_settings = { sandbox_mode: { enable: true } };
  }
  if (attachments && attachments.length) {
    body.attachments = attachments.map(att => ({
      content: Buffer.isBuffer(att.content) ? att.content.toString('base64') : Buffer.from(String(att.content || ''), 'utf8').toString('base64'),
      type: att.contentType || 'application/octet-stream',
      filename: att.filename || 'attachment',
      disposition: 'attachment',
    }));
  }
  const res = await fetch('https://api.sendgrid.com/v3/mail/send', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${key}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  });
  if (res.status >= 200 && res.status < 300) {
    return { messageId: res.headers.get('x-message-id') || null, provider: 'sendgrid' };
  }
  const text = await res.text().catch(() => '');
  const err = new Error(`SendGrid API ${res.status}: ${text}`);
  err.code = 'EMAIL_API_ERROR';
  throw err;
}

async function sendEmail({ to, cc = [], bcc = [], subject, html, attachments = [] }) {
  if (!to || (Array.isArray(to) && to.length === 0)) {
    throw new Error('No recipients provided');
  }
  // If an email API key is present, prefer it (avoids SMTP egress blocks)
  if (process.env.SENDGRID_API_KEY) {
    return await sendViaSendGrid({ to, cc, bcc, subject, html, attachments });
  }
  return await sendViaSmtp({ to, cc, bcc, subject, html, attachments });
}

async function verifySmtp() {
  const transporter = createTransporter();
  try {
    const result = await transporter.verify();
    return { ok: !!result };
  } catch (e) {
    const err = new Error(e && e.message ? e.message : 'SMTP verify failed');
    err.code = e && e.code;
    throw err;
  }
}

module.exports = { sendEmail, verifySmtp };
