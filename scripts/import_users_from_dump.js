#!/usr/bin/env node
/**
 * Import users from a pg_dump SQL file that contains a COPY block for public.users.
 * Default dump path: ../postgresql-project/crm_backup.sql
 *
 * This script parses the COPY block and inserts rows into the existing users table.
 * - Handles \N as NULL and basic backslash escapes (\\, \t, \n, \r). 
 * - Uses ON CONFLICT (id) DO NOTHING, so it's safe to re-run.
 * - Wraps all inserts in a single transaction for speed.
 *
 * Usage:
 *   DATABASE_URL=... node scripts/import_users_from_dump.js [path-to-dump.sql]
 */
const fs = require('fs');
const path = require('path');
const dotenv = require('dotenv');
const { Pool } = require('pg');
dotenv.config();

const dumpPath = process.argv[2] || path.resolve(__dirname, '..', '..', 'postgresql-project', 'crm_backup.sql');

function unescapeCopyField(field) {
  // Handle PostgreSQL COPY text format escapes
  // https://www.postgresql.org/docs/current/sql-copy.html#id-1.9.3.55.10
  if (field === '\\N') return null; // NULL
  let out = '';
  for (let i = 0; i < field.length; i++) {
    const c = field[i];
    if (c === '\\') {
      const n = field[i + 1];
      if (n === 'n') { out += '\n'; i++; continue; }
      if (n === 'r') { out += '\r'; i++; continue; }
      if (n === 't') { out += '\t'; i++; continue; }
      if (n === '\\') { out += '\\'; i++; continue; }
      // Otherwise keep the following char as-is (e.g., escapes not used here)
      // Fall-through: skip the backslash but keep next char
      if (typeof n === 'string') { out += n; i++; continue; }
      // lone backslash
      out += '\\';
    } else {
      out += c;
    }
  }
  return out;
}

async function main() {
  if (!fs.existsSync(dumpPath)) {
    console.error('Dump file not found:', dumpPath);
    process.exit(2);
  }
  const raw = fs.readFileSync(dumpPath, 'utf8');
  const startMarker = 'COPY public.users (id, email, full_name, role, password_hash, created_at, last_login, active, username, phone, must_change_password, last_password_change_at, joining_date, status) FROM stdin;';
  const startIdx = raw.indexOf(startMarker);
  if (startIdx === -1) {
    console.error('Could not find COPY block for public.users in dump');
    process.exit(2);
  }
  const afterStart = raw.slice(startIdx + startMarker.length);
  // Read line-by-line until we encounter a line that's exactly "\\."
  const rawLines = afterStart.split('\n');
  const lines = [];
  for (const ln of rawLines) {
    const trimmed = ln.replace(/\r$/, '');
    if (trimmed === '\\.') break;
    if (trimmed.length === 0) continue; // skip blank lines between
    lines.push(trimmed);
  }
  if (!lines.length) {
    console.error('Malformed COPY block: missing end \\.' );
    process.exit(2);
  }

  console.log(`Found ${lines.length} user row(s) in dump. Importing...`);

  const connectionString = process.env.DATABASE_URL || process.argv[3];
  if (!connectionString) {
    console.error('DATABASE_URL not provided. Set env DATABASE_URL or pass as 2nd arg.');
    process.exit(2);
  }
  // Neon requires SSL; enable SSL explicitly.
  const pool = new Pool({ connectionString, ssl: { rejectUnauthorized: false } });
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const line of lines) {
      const fields = line.split('\t').map(unescapeCopyField);
      if (!fields.length) continue;
      const [id, email, full_name, role, password_hash, created_at, last_login, active, username, phone, must_change_password, last_password_change_at, joining_date, status] = fields;
      // Coerce booleans
      const activeBool = active == null ? true : (active === 't' || active === 'true' || active === true);
      const mustChangeBool = must_change_password == null ? false : (must_change_password === 't' || must_change_password === 'true' || must_change_password === true);

      // Insert; if status is null, let DB default apply
      const sql = `INSERT INTO public.users
        (id, email, full_name, role, password_hash, created_at, last_login, active, username, phone, must_change_password, last_password_change_at, joining_date, status)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
        ON CONFLICT (id) DO NOTHING`;
      const params = [
        id, email && email.toLowerCase(), full_name, role, password_hash,
        created_at, last_login, activeBool, username, phone, mustChangeBool,
        last_password_change_at, joining_date, status
      ];
      await client.query(sql, params);
    }
    await client.query('COMMIT');
    console.log('Users import complete.');
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch {}
    console.error('Import failed:', err.message);
    process.exit(1);
  } finally {
    client.release();
    await pool.end();
  }
}

main();
