/**
 * One-time normalization for meetings.assigned_to and meetings.created_by
 * Goal: Map display names/emails to a canonical username/email from users table
 * so employee visibility filters work consistently.
 *
 * Safe to run multiple times; only updates rows when a clear, unambiguous mapping exists.
 */

const { Client } = require('pg');

async function main() {
  const client = new Client({
    host: process.env.PGHOST || 'localhost',
    port: +(process.env.PGPORT || 5432),
    user: process.env.PGUSER || 'postgres',
    // Align with server/db.js default so the script works in the same env
    password: process.env.PGPASSWORD || 'root123',
    database: process.env.PGDATABASE || 'crm_db',
  });

  await client.connect();

  try {
    console.log('[normalize] Loading users...');
    const usersRes = await client.query(
      `select id, coalesce(nullif(trim(username), ''), null) as username,
              coalesce(nullif(trim(email), ''), null) as email,
              coalesce(nullif(trim(full_name), ''), null) as full_name,
              role
         from users`
    );
    const users = usersRes.rows;

    // Build lookup maps
    const byLower = new Map();
    for (const u of users) {
      const keys = new Set([
        u.username, u.email, u.full_name,
        u.username && u.username.toLowerCase(),
        u.email && u.email.toLowerCase(),
        u.full_name && u.full_name.toLowerCase(),
      ].filter(Boolean));
      for (const k of keys) byLower.set(k.toLowerCase(), u);
    }

    // Helper to resolve a display value -> canonical string (prefer email, else username)
    const resolve = (val) => {
      if (!val) return null;
      const key = String(val).trim();
      if (!key) return null;
      const u = byLower.get(key.toLowerCase());
      if (!u) return null;
      return u.email || u.username || u.full_name || null;
    };

    console.log('[normalize] Scanning meetings...');
    const meetingsRes = await client.query(
      `select id, assigned_to, created_by from meetings`
    );

    let updates = 0;
    for (const m of meetingsRes.rows) {
      const newAssigned = resolve(m.assigned_to);
      const newCreated = resolve(m.created_by);

      // Only update if resolution yields a different non-null value
      const tasks = [];
      if (newAssigned && newAssigned !== m.assigned_to) {
        tasks.push(client.query('update meetings set assigned_to = $1 where id = $2', [newAssigned, m.id]));
      }
      if (newCreated && newCreated !== m.created_by) {
        tasks.push(client.query('update meetings set created_by = $1 where id = $2', [newCreated, m.id]));
      }
      if (tasks.length) {
        await Promise.all(tasks);
        updates += tasks.length;
      }
    }

    console.log(`[normalize] Meetings normalized fields updated: ${updates}`);

    // Backfill: for reminders with NULL created_by but linked to a meeting, copy meeting.created_by
    console.log('[normalize] Backfilling reminders.created_by from linked meetings...');
    const backfill = await client.query(
      `UPDATE reminders r
         SET created_by = m.created_by
        FROM meetings m
       WHERE r.meeting_id = m.id
         AND r.created_by IS NULL
         AND m.created_by IS NOT NULL`
    );
    console.log(`[normalize] Backfilled reminders rows: ${backfill.rowCount}`);

    console.log('[normalize] Scanning reminders...');
    const remindersRes = await client.query('select id, created_by from reminders');
    let rUpdates = 0;
    for (const r of remindersRes.rows) {
      const newCreated = resolve(r.created_by);
      if (newCreated && newCreated !== r.created_by) {
        await client.query('update reminders set created_by = $1 where id = $2', [newCreated, r.id]);
        rUpdates++;
      }
    }
    console.log(`[normalize] Reminders normalized fields updated: ${rUpdates}`);

    console.log('[normalize] Done.');
  } catch (err) {
    console.error('[normalize] Failed:', err);
    process.exitCode = 1;
  } finally {
    await client.end();
  }
}

main();
