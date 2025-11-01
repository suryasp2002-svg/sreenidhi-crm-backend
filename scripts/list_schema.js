#!/usr/bin/env node
// Prints all tables and columns in the public schema (and views)
const { Client } = require('pg');

function buildClient() {
  if (process.env.DATABASE_URL) {
    const needsSSL = process.env.PGSSLMODE === 'require' || /sslmode=require/i.test(process.env.DATABASE_URL);
    return new Client({ connectionString: process.env.DATABASE_URL, ssl: needsSSL ? { rejectUnauthorized: false } : undefined });
  }
  return new Client({
    host: process.env.PGHOST || 'localhost',
    port: +(process.env.PGPORT || 5432),
    user: process.env.PGUSER || 'postgres',
    password: process.env.PGPASSWORD || 'root123',
    database: process.env.PGDATABASE || 'crm_db',
    ssl: String(process.env.PGSSLMODE || '').toLowerCase() === 'require' ? { rejectUnauthorized: false } : undefined,
  });
}

async function main() {
  const client = buildClient();
  await client.connect();
  // List tables and views in public schema
  const tv = await client.query(`
    SELECT table_name, table_type
      FROM information_schema.tables
     WHERE table_schema = 'public'
     ORDER BY table_type, table_name
  `);
  const cols = await client.query(`
    SELECT table_name, column_name, data_type,
           is_nullable,
           column_default,
           ordinal_position
      FROM information_schema.columns
     WHERE table_schema = 'public'
     ORDER BY table_name, ordinal_position
  `);
  // Build PK hints (optional)
  const pks = await client.query(`
    SELECT
      kcu.table_name,
      kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
     AND tc.table_schema = kcu.table_schema
   WHERE tc.table_schema = 'public'
     AND tc.constraint_type = 'PRIMARY KEY'
  `);
  const pkSet = new Set(pks.rows.map(r => `${r.table_name}::${r.column_name}`));

  const byTable = new Map();
  cols.rows.forEach(r => {
    if (!byTable.has(r.table_name)) byTable.set(r.table_name, []);
    byTable.get(r.table_name).push(r);
  });

  // Pretty print
  const pad = (s, n) => (s || '').toString().padEnd(n, ' ');
  console.log('Schema: public');
  for (const t of tv.rows) {
    console.log(`\n=== ${t.table_name} (${t.table_type}) ===`);
    const list = byTable.get(t.table_name) || [];
    if (!list.length) { console.log('  <no columns>'); continue; }
    console.log('  ' + pad('column', 28) + pad('type', 20) + pad('nullable', 10) + 'default');
    for (const c of list) {
      const name = pkSet.has(`${t.table_name}::${c.column_name}`) ? `${c.column_name} [PK]` : c.column_name;
      console.log('  ' + pad(name, 28) + pad(c.data_type, 20) + pad(c.is_nullable, 10) + (c.column_default || ''));
    }
  }
  await client.end();
}

main().catch(e => { console.error('Error:', e.message); process.exit(1); });
