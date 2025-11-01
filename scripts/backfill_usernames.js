#!/usr/bin/env node
// Backfill usernames for users where username is NULL/empty.
// Strategy: derive from email local-part or sanitized full_name, ensure uniqueness by suffixing a short hash.
const { Client } = require('pg');
const crypto = require('crypto');

function buildClient() {
  if (process.env.DATABASE_URL) {
    const needsSSL = process.env.PGSSLMODE === 'require' || /sslmode=require/i.test(process.env.DATABASE_URL);
    return new Client({ connectionString: process.env.DATABASE_URL, ssl: needsSSL ? { rejectUnauthorized: false } : undefined });
  }
  return new Client({
    host: process.env.PGHOST || 'localhost',
    port: +(process.env.PGPORT || 5432),
    user: process.env.PGUSER || 'postgres',
    password: process.env.PGPASSWORD || 'root123',
    database: process.env.PGDATABASE || 'crm_db',
    ssl: String(process.env.PGSSLMODE || '').toLowerCase() === 'require' ? { rejectUnauthorized: false } : undefined,
  });
}

function sanitizeBase(s) {
  return String(s || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_\.-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 24);
}

async function main() {
  const client = buildClient();
  await client.connect();
  try {
    const users = await client.query(`
      SELECT id, email, username, full_name
        FROM public.users
       ORDER BY created_at ASC
    `);
    // Track existing usernames (case-insensitive)
    const taken = new Set(
      users.rows
        .map(u => (u.username || '').toLowerCase().trim())
        .filter(Boolean)
    );
    let updates = 0;
    for (const u of users.rows) {
      if (u.username && String(u.username).trim().length) continue; // already has one
      let base = null;
      if (u.email && String(u.email).includes('@')) base = String(u.email).split('@')[0];
      if (!base && u.full_name) base = String(u.full_name).replace(/\s+/g, '.');
      if (!base) base = 'user';
      base = sanitizeBase(base) || 'user';
      let candidate = base;
      let cLower = candidate.toLowerCase();
      if (taken.has(cLower)) {
        const suffix = crypto.createHash('md5').update(String(u.id)).digest('hex').slice(0, 4);
        candidate = `${base}.${suffix}`;
        cLower = candidate.toLowerCase();
        // Very unlikely second collision; if present, append a random byte
        if (taken.has(cLower)) {
          const extra = crypto.randomBytes(1).toString('hex');
          candidate = `${base}.${suffix}${extra}`;
          cLower = candidate.toLowerCase();
        }
      }
      await client.query('UPDATE public.users SET username=$1 WHERE id=$2', [candidate, u.id]);
      taken.add(cLower);
      updates++;
    }
    console.log(`[backfill_usernames] Updated users without username: ${updates}`);
  } catch (e) {
    console.error('[backfill_usernames] Failed:', e.message);
    process.exitCode = 1;
  } finally {
    await client.end();
  }
}

main();
