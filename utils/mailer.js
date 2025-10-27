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
  return { host, port, secure, auth: { user, pass } };
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

async function sendEmail({ to, cc = [], bcc = [], subject, html, attachments = [] }) {
  if (!to || (Array.isArray(to) && to.length === 0)) {
    throw new Error('No recipients provided');
  }
  const transporter = createTransporter();
  const info = await transporter.sendMail({
    from: process.env.MAIL_FROM || process.env.SMTP_USER,
    to: Array.isArray(to) ? to.join(',') : to,
    cc: cc && cc.length ? (Array.isArray(cc) ? cc.join(',') : cc) : undefined,
    bcc: bcc && bcc.length ? (Array.isArray(bcc) ? bcc.join(',') : bcc) : undefined,
    subject,
    html,
    attachments,
  });
  return info;
}

async function getSmtpStatus() {
  const cfg = buildTransportConfig();
  const missing = validateSmtpConfig(cfg);
  const summary = {
    configured: missing.length === 0,
    missing,
    transport: {
      host: cfg.host || null,
      port: cfg.port || null,
      secure: !!cfg.secure,
      user: (cfg.auth && cfg.auth.user) || null
    },
    verifyOk: false,
    message: ''
  };
  if (summary.configured) {
    try {
      const transporter = nodemailer.createTransport(cfg);
      await transporter.verify();
      summary.verifyOk = true;
      summary.message = 'SMTP connection verified';
    } catch (e) {
      summary.verifyOk = false;
      summary.message = e.message || String(e);
    }
  } else {
    summary.message = `Missing ${missing.join(', ')}`;
  }
  return summary;
}

module.exports = { sendEmail, getSmtpStatus };
