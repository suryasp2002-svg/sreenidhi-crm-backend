// One-off script to populate NULL emails before running migration
const conn = process.argv[2];
if (conn && !process.env.DATABASE_URL) process.env.DATABASE_URL = conn;
const pool = require('./db');
(async () => {
  try {
    const q = `UPDATE users SET email = COALESCE(email, CASE WHEN username IS NOT NULL THEN username || '@example.invalid' ELSE id::text || '@example.invalid' END) WHERE email IS NULL`;
    const res = await pool.query(q);
    console.log('[fix_null_emails] Updated rows', res.rowCount);
    process.exit(0);
  } catch (e) {
    console.error('[fix_null_emails] Error', e);
    process.exit(1);
  }
})();
