const pool = require('../db');

(async () => {
  const client = await pool.connect();
  try {
    const dbInfo = await client.query("SELECT current_database() AS db, current_schema() AS schema");
    const exists = await client.query(
      "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = 'targets') AS present"
    );
    const list = await client.query(
      "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = current_schema() AND table_name IN ('targets','opportunities','customers','contracts','reminders') ORDER BY table_name"
    );
    console.log('Database:', dbInfo.rows[0]);
    console.log('Targets table present:', exists.rows[0].present);
    console.log('Known tables in current schema:', list.rows);
    process.exit(0);
  } catch (e) {
    console.error(e);
    process.exit(1);
  } finally {
    client.release();
  }
})();
