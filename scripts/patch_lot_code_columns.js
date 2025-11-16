// Reconcile lot_code_initial vs lot_code_created in public.fuel_lots
// - If both exist: backfill initial from created when null and drop NOT NULL on initial
// - Ensure unique index on lot_code_created
// - Optionally rename when only initial exists

const pool = require('../db');

async function columnExists(client, table, column) {
  const q = await client.query(
    `SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name=$1 AND column_name=$2`,
    [table, column]
  );
  return q.rows.length > 0;
}

async function indexExists(client, indexName) {
  const q = await client.query(
    `SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname=$1`,
    [indexName]
  );
  return q.rows.length > 0;
}

async function run() {
  const c = await pool.connect();
  try {
    await c.query('BEGIN');

    const hasInitial = await columnExists(c, 'fuel_lots', 'lot_code_initial');
    const hasCreated = await columnExists(c, 'fuel_lots', 'lot_code_created');

    console.log('[patch] lot_code_initial:', hasInitial, 'lot_code_created:', hasCreated);

    if (hasInitial && hasCreated) {
      // Backfill any nulls and drop NOT NULL from lot_code_initial to unblock inserts that only set created
      await c.query(`UPDATE public.fuel_lots SET lot_code_initial = COALESCE(lot_code_initial, lot_code_created)`);
      // Drop NOT NULL if present
      try {
        await c.query(`ALTER TABLE public.fuel_lots ALTER COLUMN lot_code_initial DROP NOT NULL`);
        console.log('[patch] Dropped NOT NULL on lot_code_initial');
      } catch (e) {
        console.log('[patch] Could not drop NOT NULL on lot_code_initial (might already be nullable):', e.message);
      }
    } else if (hasInitial && !hasCreated) {
      // Rename initial -> created
      try {
        await c.query(`ALTER TABLE public.fuel_lots RENAME COLUMN lot_code_initial TO lot_code_created`);
        console.log('[patch] Renamed lot_code_initial -> lot_code_created');
      } catch (e) {
        console.log('[patch] Rename failed:', e.message);
      }
    }

    // Ensure unique index on lot_code_created
    const hasIdx = await indexExists(c, 'uq_fuel_lots_created_code');
    if (!hasIdx) {
      try {
        await c.query(`CREATE UNIQUE INDEX uq_fuel_lots_created_code ON public.fuel_lots(lot_code_created)`);
        console.log('[patch] Created unique index uq_fuel_lots_created_code');
      } catch (e) {
        console.log('[patch] Unique index create skipped/failed:', e.message);
      }
    }

    await c.query('COMMIT');
    console.log('[patch] Completed successfully');
  } catch (e) {
    try { await c.query('ROLLBACK'); } catch {}
    console.error('[patch] Failed:', e);
    process.exitCode = 1;
  } finally {
    c.release();
  }
}

run();
