// Demo data seeding for Sreenidhi CRM
const pool = require('./db');

async function seed() {
  // Customers
  await pool.query(`INSERT INTO customers (id, legal_name, gstin, primary_contact, phone, email, created_at)
    VALUES
      ('MEDH01', 'Medha Engineering Pvt Ltd', '37ABCDE1234F1Z5', 'Rakesh Rao', '+91 9876511111', 'rakesh@medha.co.in', NOW()),
      ('RENW02', 'RenewSys India', '27ABCDE9999L1Z1', 'S. Iyer', '+91 9898922222', 'si@renewsys.in', NOW())
    ON CONFLICT (id) DO NOTHING;
  `);

  // Contracts
  await pool.query(`INSERT INTO contracts (id, customer_id, quoted_price_per_litre, start_date, payment_terms, primary_contact, phone, email, gstin)
    VALUES
      ('CON-RENW02', 'RENW02', 101.5, CURRENT_DATE, 'Net 15', 'S. Iyer', '+91 9898922222', 'si@renewsys.in', '27ABCDE9999L1Z1')
    ON CONFLICT (id) DO NOTHING;
  `);

  // Opportunities
  await pool.query(`INSERT INTO opportunities (id, customer_id, title, expected_monthly_volume_l, proposed_price_per_litre, stage, probability, notes, salesperson)
    VALUES
      ('OPP-1', 'MEDH01', 'HSD Monthly Supply', 45000, 96.5, 'NEGOTIATION', 70, '30d credit', 'Anita'),
      ('OPP-2', 'RENW02', 'MS Daily Runs', 20000, 102.0, 'AGREED', 100, 'PO pending', 'Rahul')
    ON CONFLICT (id) DO NOTHING;
  `);

  // Status History
  await pool.query(`INSERT INTO status_history (opportunity_id, stage, reason, at)
    VALUES ('OPP-2', 'AGREED', '', NOW())
    ON CONFLICT DO NOTHING;
  `);

  // No demo expenses, meetings, reminders for now
  console.log('Demo data seeded.');
  process.exit(0);
}

seed().catch(e => { console.error(e); process.exit(1); });
