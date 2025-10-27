const pool = require('../db');

async function main() {
  const out = { users: {}, tables: {}, user_profiles: {}, user_photos: {} };
  try {
    // Users columns
    const usersCols = await pool.query(`
      SELECT column_name FROM information_schema.columns
      WHERE table_schema='public' AND table_name='users'
        AND column_name IN ('joining_date','status')
      ORDER BY column_name
    `);
    out.users.columns = usersCols.rows.map(r => r.column_name);
    // Status check constraint existence
    const statusCk = await pool.query(`
      SELECT 1
      FROM pg_constraint
      WHERE conname = 'users_status_check'
    `);
    out.users.status_check_constraint = statusCk.rows.length > 0;

    // Tables existence
    const t1 = await pool.query(`SELECT to_regclass('public.user_profiles') AS reg`);
    const t2 = await pool.query(`SELECT to_regclass('public.user_photos') AS reg`);
    out.tables.user_profiles = !!(t1.rows && t1.rows[0] && t1.rows[0].reg);
    out.tables.user_photos = !!(t2.rows && t2.rows[0] && t2.rows[0].reg);

    // user_profiles columns
    if (out.tables.user_profiles) {
      const profCols = await pool.query(`
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name='user_profiles'
        ORDER BY column_name
      `);
      out.user_profiles.columns = profCols.rows.map(r => r.column_name);
    }

    // user_photos columns
    if (out.tables.user_photos) {
      const photoCols = await pool.query(`
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name='user_photos'
        ORDER BY column_name
      `);
      out.user_photos.columns = photoCols.rows.map(r => r.column_name);
    }

    console.log(JSON.stringify(out, null, 2));
    process.exit(0);
  } catch (e) {
    console.error('Schema check failed:', e.message);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

main();
