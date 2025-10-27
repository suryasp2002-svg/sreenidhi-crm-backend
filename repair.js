// repair.js - Fix customers.opportunity_id mismatches by matching client_name to opportunities
const pool = require('./db');

async function run() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const reportBefore = await client.query(`
      SELECT COUNT(*) AS mismatches
      FROM customers c
      LEFT JOIN opportunities o ON o.opportunity_id = c.opportunity_id
      WHERE o.opportunity_id IS NULL;
    `);
    console.log('Mismatches before:', reportBefore.rows[0].mismatches);

    const res = await client.query(`
      WITH mismatched AS (
        SELECT c.customer_id, c.client_name, c.opportunity_id AS old_opp
        FROM customers c
        LEFT JOIN opportunities o ON o.opportunity_id = c.opportunity_id
        WHERE o.opportunity_id IS NULL
      ),
      candidates AS (
        SELECT m.customer_id, o.opportunity_id AS new_opp
        FROM mismatched m
        JOIN opportunities o ON LOWER(o.client_name) = LOWER(m.client_name)
      ),
      unique_only AS (
        SELECT customer_id, new_opp
        FROM (
          SELECT customer_id, new_opp, COUNT(*) OVER (PARTITION BY customer_id) AS cnt
          FROM candidates
        ) t
        WHERE cnt = 1
      ),
      do_update AS (
        UPDATE customers c
        SET opportunity_id = u.new_opp
        FROM unique_only u
        WHERE c.customer_id = u.customer_id
        RETURNING c.customer_id, u.new_opp
      )
      SELECT COUNT(*) AS updated FROM do_update;
    `);
    console.log('Customers updated:', res.rows[0].updated);

    const unresolved = await client.query(`
      SELECT m.customer_id, m.client_name, m.old_opp
      FROM (
        SELECT c.customer_id, c.client_name, c.opportunity_id AS old_opp
        FROM customers c
        LEFT JOIN opportunities o ON o.opportunity_id = c.opportunity_id
        WHERE o.opportunity_id IS NULL
      ) m
      ORDER BY m.client_name;
    `);
    if (unresolved.rows.length) {
      console.log('Unresolved (manual review needed):');
      for (const r of unresolved.rows) {
        console.log(` - ${r.client_name} | customer ${r.customer_id} | bad opp ${r.old_opp}`);
      }
    } else {
      console.log('All mismatches resolved.');
    }

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Repair failed:', err);
    process.exit(1);
  } finally {
    client.release();
    process.exit(0);
  }
}

run();
