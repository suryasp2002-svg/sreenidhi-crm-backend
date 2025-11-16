const pool = require('../db');

(async () => {
  const client = await pool.connect();
  try {
    const dbInfo = await client.query('SELECT current_database() as db, current_user as usr, inet_server_addr()::text as host, inet_server_port() as port');
    console.log('[DB]', dbInfo.rows[0]);

    const tables = ['storage_units','fuel_lots','fuel_internal_transfers'];
    for (const t of tables) {
      const q = await client.query(`
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema='public' AND table_name=$1
         ORDER BY ordinal_position;
      `, [t]);
      console.log(`${t} columns:`, q.rows.map(r => r.column_name));
    }
  } catch (e) {
    console.error('Error:', e.message);
    process.exitCode = 1;
  } finally {
    client.release();
    pool.end();
  }
})();
