#!/usr/bin/env node
const { Client } = require('pg');

async function main() {
  const url = process.argv[2] || process.env.DATABASE_URL;
  if (!url) {
    console.error('Usage: node scripts/db_counts.js <DATABASE_URL>');
    process.exit(1);
  }
  const client = new Client({ connectionString: url, ssl: { rejectUnauthorized: false } });
  await client.connect();
  const tables = ['opportunities','customers','contracts','users','meetings','reminders','expenses'];
  for (const t of tables) {
    try {
      const r = await client.query(`SELECT COUNT(*)::int AS n FROM public.${t}`);
      console.log(`${t}:`, r.rows[0].n);
    } catch (e) {
      console.log(`${t}: error - ${e.message}`);
    }
  }
  await client.end();
}
main().catch(e => { console.error('Error:', e.message); process.exit(1); });
