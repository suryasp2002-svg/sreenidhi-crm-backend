// Run a single .sql file against the configured database
const fs = require('fs');
const path = require('path');
const pool = require('../db');

async function main() {
  const rel = process.argv[2];
  if (!rel) {
    console.error('Usage: node scripts/run_sql_file.js <relative-path-to-sql>');
    process.exit(2);
  }
  const full = path.isAbsolute(rel) ? rel : path.join(__dirname, '..', rel);
  if (!fs.existsSync(full)) {
    console.error('File not found:', full);
    process.exit(2);
  }
  const sql = fs.readFileSync(full, 'utf8');
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(sql);
    await client.query('COMMIT');
    console.log('[run-sql] Applied', path.basename(full));
    process.exit(0);
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch(_) {}
    console.error('[run-sql] Failed:', err.message || err);
    process.exit(1);
  } finally {
    client.release();
  }
}

main();
