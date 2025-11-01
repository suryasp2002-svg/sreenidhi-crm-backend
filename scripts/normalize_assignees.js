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

    // Helper to resolve a display value -> canonical string (prefer USERNAME ONLY)
    const resolve = (val) => {
      if (!val) return null;
      const key = String(val).trim();
      if (!key) return null;
      const u = byLower.get(key.toLowerCase());
      if (!u) return null;
      return u.username || null;
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
    const remindersRes = await client.query('select id, created_by, assigned_to, assigned_to_user_id from reminders');
    let rUpdates = 0;
    for (const r of remindersRes.rows) {
      const newCreated = resolve(r.created_by);
      let newAssigned = resolve(r.assigned_to);
      // If not resolvable by string, but we have an assigned_to_user_id, derive from that user
      if ((!newAssigned || newAssigned === r.assigned_to) && r.assigned_to_user_id) {
        try {
          const ures = await client.query('select username from users where id = $1', [r.assigned_to_user_id]);
          if (ures.rows.length && ures.rows[0].username) {
            const uname = ures.rows[0].username;
            if (uname && uname !== r.assigned_to) newAssigned = uname;
          }
        } catch {}
      }
      const tasks = [];
      if (newCreated && newCreated !== r.created_by) tasks.push(client.query('update reminders set created_by = $1 where id = $2', [newCreated, r.id]));
      if (newAssigned && newAssigned !== r.assigned_to) tasks.push(client.query('update reminders set assigned_to = $1 where id = $2', [newAssigned, r.id]));
      if (tasks.length) {
        await Promise.all(tasks);
        rUpdates += tasks.length;
      }
    }
    console.log(`[normalize] Reminders normalized fields updated: ${rUpdates}`);

    // Hard-sync pass: use assigned_to_user_id join to overwrite mismatches in one SQL
    try {
      const syncR = await client.query(`
        UPDATE reminders r
           SET assigned_to = u.username
          FROM users u
         WHERE r.assigned_to_user_id = u.id
           AND u.username IS NOT NULL
           AND u.username <> r.assigned_to
      `);
      console.log(`[normalize] Reminders hard-sync from user_id updated: ${syncR.rowCount}`);
    } catch (e) {
      console.log('[normalize] Reminders hard-sync skipped:', e.message);
    }

    // Fallback sync: if assigned_to matches a user's email, replace with that user's username
    try {
      const syncREmail = await client.query(`
        UPDATE reminders r
           SET assigned_to = u.username
          FROM users u
         WHERE u.username IS NOT NULL
           AND LOWER(COALESCE(r.assigned_to,'')) = LOWER(COALESCE(u.email,''))
           AND u.username <> r.assigned_to
      `);
      console.log(`[normalize] Reminders email->username sync updated: ${syncREmail.rowCount}`);
    } catch (e) {
      console.log('[normalize] Reminders email->username sync skipped:', e.message);
    }

    // Normalize audit tables to store username in actor fields
    // 1) expenses_audit.performed_by
    try {
      console.log('[normalize] Scanning expenses_audit...');
      const ea = await client.query('select id, performed_by from expenses_audit');
      let eUpdates = 0;
      for (const row of ea.rows) {
        const v = resolve(row.performed_by);
        if (v && v !== row.performed_by) {
          await client.query('update expenses_audit set performed_by = $1 where id = $2', [v, row.id]);
          eUpdates++;
        }
      }
      console.log(`[normalize] expenses_audit updated: ${eUpdates}`);
    } catch (e) {
      console.log('[normalize] expenses_audit skipped:', e.message);
    }

    // 2) contract_status_audit.changed_by
    try {
      console.log('[normalize] Scanning contract_status_audit...');
      const ca = await client.query('select id, changed_by from contract_status_audit');
      let cUpdates = 0;
      for (const row of ca.rows) {
        const v = resolve(row.changed_by);
        if (v && v !== row.changed_by) {
          await client.query('update contract_status_audit set changed_by = $1 where id = $2', [v, row.id]);
          cUpdates++;
        }
      }
      console.log(`[normalize] contract_status_audit updated: ${cUpdates}`);
    } catch (e) {
      console.log('[normalize] contract_status_audit skipped:', e.message);
    }

    // 3) customer_status_audit.changed_by
    try {
      console.log('[normalize] Scanning customer_status_audit...');
      const cua = await client.query('select id, changed_by from customer_status_audit');
      let cuUpdates = 0;
      for (const row of cua.rows) {
        const v = resolve(row.changed_by);
        if (v && v !== row.changed_by) {
          await client.query('update customer_status_audit set changed_by = $1 where id = $2', [v, row.id]);
          cuUpdates++;
        }
      }
      console.log(`[normalize] customer_status_audit updated: ${cuUpdates}`);
    } catch (e) {
      console.log('[normalize] customer_status_audit skipped:', e.message);
    }

    // 4) opportunity_stage_audit.changed_by
    try {
      console.log('[normalize] Scanning opportunity_stage_audit...');
      const oa = await client.query('select id, changed_by from opportunity_stage_audit');
      let oUpdates = 0;
      for (const row of oa.rows) {
        const v = resolve(row.changed_by);
        if (v && v !== row.changed_by) {
          await client.query('update opportunity_stage_audit set changed_by = $1 where id = $2', [v, row.id]);
          oUpdates++;
        }
      }
      console.log(`[normalize] opportunity_stage_audit updated: ${oUpdates}`);
    } catch (e) {
      console.log('[normalize] opportunity_stage_audit skipped:', e.message);
    }

    // Normalize targets.assigned_to to username
    try {
      console.log('[normalize] Scanning targets...');
      const targetsRes = await client.query('select id, assigned_to from targets');
      let tUpdates = 0;
      for (const t of targetsRes.rows) {
        const newAssigned = resolve(t.assigned_to);
        if (newAssigned && newAssigned !== t.assigned_to) {
          await client.query('update targets set assigned_to = $1 where id = $2', [newAssigned, t.id]);
          tUpdates++;
        }
      }
      console.log(`[normalize] Targets normalized fields updated: ${tUpdates}`);
    } catch (e) {
      console.log('[normalize] Targets table not present or skipped:', e.message);
    }

    // 5) users_password_audit.changed_by -> username
    try {
      console.log('[normalize] Scanning users_password_audit...');
      const upa = await client.query('select id, changed_by, changed_by_user_id from users_password_audit');
      let upaUpdates = 0;
      for (const row of upa.rows) {
        let v = resolve(row.changed_by);
        // If unresolved but we have user_id, derive from users table
        if ((!v || v === row.changed_by) && row.changed_by_user_id) {
          try {
            const ur = await client.query('select username from users where id = $1', [row.changed_by_user_id]);
            if (ur.rows.length && ur.rows[0].username) v = ur.rows[0].username;
          } catch {}
        }
        if (v && v !== row.changed_by) {
          await client.query('update users_password_audit set changed_by = $1 where id = $2', [v, row.id]);
          upaUpdates++;
        }
      }
      console.log(`[normalize] users_password_audit updated: ${upaUpdates}`);

      // Hard-sync by user_id in bulk
      try {
        const s1 = await client.query(`
          UPDATE users_password_audit a
             SET changed_by = u.username
            FROM users u
           WHERE a.changed_by_user_id = u.id
             AND u.username IS NOT NULL
             AND COALESCE(a.changed_by,'') <> u.username
        `);
        console.log(`[normalize] users_password_audit hard-sync from user_id updated: ${s1.rowCount}`);
      } catch (e) {
        console.log('[normalize] users_password_audit hard-sync skipped:', e.message);
      }

      // Fallback: email -> username mapping
      try {
        const s2 = await client.query(`
          UPDATE users_password_audit a
             SET changed_by = u.username
            FROM users u
           WHERE u.username IS NOT NULL
             AND LOWER(COALESCE(a.changed_by,'')) = LOWER(COALESCE(u.email,''))
             AND COALESCE(a.changed_by,'') <> u.username
        `);
        console.log(`[normalize] users_password_audit email->username sync updated: ${s2.rowCount}`);
      } catch (e) {
        console.log('[normalize] users_password_audit email->username sync skipped:', e.message);
      }
    } catch (e) {
      console.log('[normalize] users_password_audit skipped:', e.message);
    }

    console.log('[normalize] Done.');
  } catch (err) {
    console.error('[normalize] Failed:', err);
    process.exitCode = 1;
  } finally {
    await client.end();
  }
}

main();
