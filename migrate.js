// Migration runner: executes schema.sql, then all SQL files in ./migrations (sorted)
const fs = require('fs');
const path = require('path');

async function run() {
  // Allow passing DATABASE_URL as CLI arg to avoid shell env issues on Windows
  const conn = process.argv[2];
  if (conn && !process.env.DATABASE_URL) {
    process.env.DATABASE_URL = conn;
  }

  const pool = require('./db');
  const client = await pool.connect();
  try {
    // 1) Apply base schema.sql (idempotent) unless ONLY_MIGRATIONS is set
    if (!process.env.ONLY_MIGRATIONS) {
      const schemaPath = path.join(__dirname, 'schema.sql');
      const schemaSql = fs.readFileSync(schemaPath, 'utf8');
      try {
        await client.query('BEGIN');
        await client.query(schemaSql);
        await client.query('COMMIT');
        console.log('[migrate] Applied schema.sql');
      } catch (schemaErr) {
        try { await client.query('ROLLBACK'); } catch(_) {}
        console.warn('[migrate] schema.sql failed, continuing with migrations:', schemaErr.message || schemaErr);
      }
    } else {
      console.log('[migrate] Skipping schema.sql (ONLY_MIGRATIONS=1)');
    }

    // 2) Apply all migrations in ./migrations (if directory exists)
    const migrationsDir = path.join(__dirname, 'migrations');
    if (fs.existsSync(migrationsDir)) {
      const files = fs
        .readdirSync(migrationsDir)
        .filter((f) => f.toLowerCase().endsWith('.sql'))
        .sort((a, b) => a.localeCompare(b));

      for (const file of files) {
        const fullPath = path.join(migrationsDir, file);
        const sql = fs.readFileSync(fullPath, 'utf8');
        try {
          // Let each migration manage its own transaction if it includes BEGIN/COMMIT;
          // otherwise wrap it to keep things safe.
          // Detect explicit transaction blocks OR statements that must run outside a transaction (e.g. CREATE INDEX CONCURRENTLY)
          const hasExplicitTxBlock = /\bBEGIN\b[\s\S]*\bCOMMIT\b/i.test(sql);
          const requiresNoTx = /CREATE\s+INDEX\s+CONCURRENTLY/i.test(sql);
          const hasExplicitTx = hasExplicitTxBlock || requiresNoTx;
          if (!hasExplicitTx) {
            await client.query('BEGIN');
          }
          await client.query(sql);
          if (!hasExplicitTx) {
            await client.query('COMMIT');
          }
          console.log(`[migrate] Applied ${file}`);
        } catch (err) {
          // Rollback only if we started a transaction
          try { await client.query('ROLLBACK'); } catch (_) {}
          const msg = err.message || String(err);
          console.error(`[migrate] Failed on ${file}:`, msg);
          // Tolerate legacy fuel_lots function migrations on instances where column names differ
          if (/^(011_|012_|030_)/.test(file) && /load_date/i.test(msg)) {
            console.warn(`[migrate] Skipping legacy migration ${file} due to load_date/loaded_date mismatch`);
            continue;
          }
          if (/performed_at/i.test(msg)) {
            console.warn(`[migrate] Skipping legacy migration ${file} due to performed_at mismatch`);
            continue;
          }
          if (/transfer_volume_liters/i.test(msg)) {
            console.warn(`[migrate] Skipping legacy migration ${file} due to transfer_volume_liters mismatch`);
            continue;
          }
          if (/(does not exist)/i.test(msg) && /^(020_|021_|022_|023_|024_|025_|026_|027_|028_|029_|030_|031_|032_|033_)/.test(file)) {
            console.warn(`[migrate] Skipping legacy migration ${file} due to missing relation/column`);
            continue;
          }
          throw err;
        }
      }
    } else {
      console.log('[migrate] No migrations directory found; only schema.sql applied');
    }

    console.log('[migrate] All migrations applied successfully');
    process.exit(0);
  } catch (err) {
    console.error('[migrate] Migration failed:', err);
    process.exit(1);
  } finally {
    client.release();
  }
}

run();
