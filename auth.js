// auth.js - helper functions and middleware for authentication & authorization (Phase 1)
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');
const pool = require('./db');

const JWT_SECRET = process.env.JWT_SECRET || 'dev_secret_change_me';
const JWT_EXPIRES_IN = '12h';

async function hashPassword(plain) {
  const saltRounds = 10;
  return bcrypt.hash(plain, saltRounds);
}

async function verifyPassword(plain, hash) {
  return bcrypt.compare(plain, hash);
}

function signToken(user) {
  return jwt.sign({
    sub: user.id,
    role: user.role,
    email: user.email,
    username: user.username || undefined
  }, JWT_SECRET, { expiresIn: JWT_EXPIRES_IN });
}

async function loadUserById(id) {
  const r = await pool.query('SELECT id, email, full_name, role, created_at, last_login, active FROM users WHERE id=$1 AND active = TRUE', [id]);
  return r.rows[0] || null;
}

function requireAuth(req, res, next) {
  const auth = req.headers.authorization || '';
  if (!auth.startsWith('Bearer ')) return res.status(401).json({ error: 'Missing token' });
  const token = auth.slice(7);
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    req.user = decoded; // sub, role, email
    next();
  } catch (err) {
    return res.status(401).json({ error: 'Invalid or expired token' });
  }
}

function requireRole(...roles) {
  return (req, res, next) => {
    if (!req.user) return res.status(401).json({ error: 'Not authenticated' });
    if (!roles.includes(req.user.role)) return res.status(403).json({ error: 'Forbidden' });
    next();
  };
}

// Utility: ensure only one OWNER (app logic enforcement) when creating
async function ownerExists() {
  const r = await pool.query("SELECT 1 FROM users WHERE role='OWNER' AND active=TRUE LIMIT 1");
  return r.rows.length > 0;
}

module.exports = {
  hashPassword,
  verifyPassword,
  signToken,
  requireAuth,
  requireRole,
  loadUserById,
  ownerExists
};
