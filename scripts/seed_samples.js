#!/usr/bin/env node
const { Pool } = require('pg');
const { randomUUID } = require('crypto');

async function main() {
  const url = process.argv[2] || process.env.DATABASE_URL;
  if (!url) {
    console.error('Usage: node scripts/seed_samples.js <DATABASE_URL>');
    process.exit(1);
  }
  const pool = new Pool({ connectionString: url, ssl: { rejectUnauthorized: false } });
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const users = await client.query("SELECT id, email, COALESCE(full_name, username, email) AS label FROM public.users WHERE active=TRUE ORDER BY role LIMIT 1");
    const user = users.rows[0];
    const salesperson = user ? (user.email) : 'owner@example.com';
    const now = new Date();
    function id(prefix) { return prefix + '-' + Math.random().toString(36).slice(2,8).toUpperCase(); }
    const opps = [
      { id: id('OPP'), name: 'Acme Constructions', purpose: 'Diesel Supply', vol: 5000, price: 92.5, stage: 'LEAD', sector: 'CONSTRUCTION' },
      { id: id('OPP'), name: 'GreenHealth Hospital', purpose: 'Generator Fuel', vol: 1500, price: 94.0, stage: 'QUALIFIED', sector: 'HOSPITAL & HEALTHCARE' },
      { id: id('OPP'), name: 'Metro Logistics', purpose: 'Fleet Fuel', vol: 8000, price: 91.5, stage: 'NEGOTIATION', sector: 'LOGISTICS' }
    ];
    for (const o of opps) {
      await client.query(
        `INSERT INTO public.opportunities (opportunity_id, client_name, purpose, expected_monthly_volume_l, proposed_price_per_litre, stage, probability, notes, salesperson, assignment, spend, loss_reason, sector, location_url)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
         ON CONFLICT (opportunity_id) DO NOTHING`,
        [o.id, o.name, o.purpose, o.vol, o.price, o.stage, o.stage==='LEAD'?10:o.stage==='QUALIFIED'?40:o.stage==='NEGOTIATION'?70:100, 'seed sample', salesperson, 'CUSTOMER', 0, null, o.sector, null]
      );
    }
    // Add a customer and contract for the second opportunity to showcase right panel
    const opp2 = opps[1].id;
    const custId = Math.random().toString(36).slice(2,8).toUpperCase();
    await client.query(`INSERT INTO public.customers (customer_id, opportunity_id, client_name, created_at, customer_status) VALUES ($1,$2,$3,NOW(),'ACTIVE') ON CONFLICT (customer_id) DO NOTHING`, [custId, opp2, 'GreenHealth Hospital']);
    const contractId = Math.random().toString(36).slice(2,8).toUpperCase();
    await client.query(`INSERT INTO public.contracts (contract_id, opportunity_id, client_name, quoted_price_per_litre, created_at, contract_status) VALUES ($1,$2,$3,$4,NOW(),'ACTIVE') ON CONFLICT (contract_id) DO NOTHING`, [contractId, opp2, 'GreenHealth Hospital', 94.0]);

    await client.query('COMMIT');
    console.log('Seeded sample opportunities:', opps.map(o=>o.id).join(', '));
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('Seeding failed:', e.message);
    process.exit(1);
  } finally {
    client.release();
    await pool.end();
  }
}
main();
