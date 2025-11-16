const pool = require('../db');

(async () => {
  const client = await pool.connect();
  try {
    const info = await client.query('SELECT current_database() as db, current_user as usr');
    console.log('[DB]', info.rows[0]);
    const q = await client.query(`
      SELECT column_name
        FROM information_schema.columns
       WHERE table_schema='public' AND table_name='fuel_sale_transfers'
       ORDER BY ordinal_position;
    `);
    const cols = q.rows.map(r => r.column_name);
    console.log('fuel_sale_transfers columns:', cols);
    console.log('trip present?', cols.includes('trip'));
  } catch (e) {
    console.error('Error:', e.message);
    process.exitCode = 1;
  } finally {
    client.release();
    pool.end();
  }
})();
