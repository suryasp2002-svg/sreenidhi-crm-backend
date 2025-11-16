// One-time backfill: set fuel_lots.load_time for lots created via EMPTY_TRANSFER using the
// corresponding internal transfer performed_at when available.
// Safe to run multiple times; only updates rows where load_time is NULL and load_type='EMPTY_TRANSFER'.

const pool = require('../db');

(async () => {
  try {
    const sql = `
      WITH candidates AS (
        SELECT fl.id AS lot_id, MIN(fit.performed_at) AS performed_at
          FROM public.fuel_lots fl
          JOIN public.fuel_internal_transfers fit ON fit.to_lot_id = fl.id
         WHERE fl.load_type = 'EMPTY_TRANSFER'
           AND fl.load_time IS NULL
           AND fit.transfer_to_empty = TRUE
         GROUP BY fl.id
      )
      UPDATE public.fuel_lots fl
         SET load_time = c.performed_at
        FROM candidates c
       WHERE fl.id = c.lot_id
         AND fl.load_time IS NULL
         AND fl.load_type = 'EMPTY_TRANSFER'
      RETURNING fl.id, fl.lot_code_initial, fl.load_time;
    `;
    const r = await pool.query(sql);
    console.log(`[backfill] Updated ${r.rowCount} EMPTY_TRANSFER lot(s) with load_time.`);
  } catch (e) {
    console.error('[backfill] Failed:', e.message);
    process.exitCode = 1;
  } finally {
    try { await pool.end(); } catch {}
  }
})();
