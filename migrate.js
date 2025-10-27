// Simple migration runner: executes schema.sql against the configured database
const fs = require('fs');
const path = require('path');

async function run() {
  // Allow passing DATABASE_URL as CLI arg to avoid shell env issues on Windows
  const conn = process.argv[2];
  if (conn && !process.env.DATABASE_URL) {
    process.env.DATABASE_URL = conn;
  }
  const pool = require('./db');
  const schemaPath = path.join(__dirname, 'schema.sql');
  const sql = fs.readFileSync(schemaPath, 'utf8');
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(sql);
    await client.query('COMMIT');
    console.log('Migration applied successfully');
    process.exit(0);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Migration failed:', err);
    process.exit(1);
  } finally {
    client.release();
  }
}

run();
