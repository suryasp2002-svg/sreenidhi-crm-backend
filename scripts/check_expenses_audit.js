const pool = require('../db');
(async () => {
  const client = await pool.connect();
  try {
    const cnt = await client.query('SELECT COUNT(*)::int AS total FROM expenses_audit');
    console.log('expenses_audit total:', cnt.rows[0].total);
    const recent = await client.query('SELECT opportunity_id, action, old_amount, new_amount, old_note, new_note, performed_by, performed_at FROM expenses_audit ORDER BY performed_at DESC LIMIT 10');
    console.log('recent rows:', recent.rows);
  } catch (e) {
    console.error('check failed:', e);
  } finally {
    client.release();
    process.exit(0);
  }
})();
