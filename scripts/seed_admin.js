#!/usr/bin/env node
/**
 * Seed an ADMIN user directly into the DB.
 * Usage:
 *   env: INIT_ADMIN_EMAIL, INIT_ADMIN_PASSWORD, INIT_ADMIN_NAME (optional)
 *   or:  node scripts/seed_admin.js <email> <password> [full name] [DATABASE_URL]
 *
 * Notes:
 * - Allows multiple ADMIN users.
 * - Sets must_change_password=false so login works immediately.
 */
const { randomUUID } = require('crypto');
const dotenv = require('dotenv');
dotenv.config();

// Allow passing DATABASE_URL as 4th CLI arg to avoid env friction
if (!process.env.DATABASE_URL && process.argv[5]) {
  process.env.DATABASE_URL = process.argv[5];
}

const pool = require('../db');
const { hashPassword } = require('../auth');

async function main() {
  try {
    const args = process.argv.slice(2);
    const email = (process.env.INIT_ADMIN_EMAIL || args[0] || '').trim().toLowerCase();
    const password = process.env.INIT_ADMIN_PASSWORD || args[1] || '';
    const fullName = process.env.INIT_ADMIN_NAME || args[2] || null;

    if (!email || !password) {
      console.error('Usage: INIT_ADMIN_EMAIL=you@example.com INIT_ADMIN_PASSWORD=secret [INIT_ADMIN_NAME="Full Name"] node scripts/seed_admin.js\n   or: node scripts/seed_admin.js <email> <password> [full name] [DATABASE_URL]');
      process.exit(2);
    }

    // Check if email already exists
    const exists = await pool.query('SELECT 1 FROM public.users WHERE email=$1', [email]);
    if (exists.rows.length) {
      console.log('User with this email already exists. Nothing to do.');
      process.exit(0);
    }

    const pwHash = await hashPassword(password);
    const newId = randomUUID();

    const sql = `INSERT INTO public.users
      (id, email, full_name, role, password_hash, must_change_password)
      VALUES ($1,$2,$3,'ADMIN',$4,false)
      RETURNING id, email, full_name, role, created_at`;
    const r = await pool.query(sql, [newId, email, fullName, pwHash]);
    const user = r.rows[0];
    console.log('Seeded ADMIN user:', user);
  } catch (err) {
    console.error('Seed failed:', err.message);
    process.exit(1);
  } finally {
    try { await pool.end(); } catch {}
  }
}

main();
