// db.js - PostgreSQL connection setup (supports Neon/SSL and DATABASE_URL)
const { Pool } = require('pg');

function buildPool() {
  // Prefer a single DATABASE_URL when provided
  if (process.env.DATABASE_URL) {
    const needsSSL = process.env.PGSSLMODE === 'require' || /sslmode=require/i.test(process.env.DATABASE_URL);
    const pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: needsSSL ? { rejectUnauthorized: false } : undefined,
      max: Number(process.env.PGPOOL_MAX || 15),
      idleTimeoutMillis: Number(process.env.PG_IDLE_TIMEOUT || 30000),
      connectionTimeoutMillis: Number(process.env.PG_CONN_TIMEOUT || 5000),
    });
    // Light diagnostic
    if (!process.env.SUPPRESS_DB_LOG) {
      console.log(`[DB] Connecting via DATABASE_URL (ssl=${needsSSL ? 'on' : 'off'})`);
    }
    return pool;
  }

  // Otherwise use discrete PG* env vars (good for local dev)
  const config = {
    user: process.env.PGUSER || 'postgres',
    host: process.env.PGHOST || 'localhost',
    database: process.env.PGDATABASE || 'crm_db',
    password: process.env.PGPASSWORD || 'root123',
    port: Number(process.env.PGPORT || 5432),
    // Pool hardening
    max: Number(process.env.PGPOOL_MAX || 15),
    idleTimeoutMillis: Number(process.env.PG_IDLE_TIMEOUT || 30000),
    connectionTimeoutMillis: Number(process.env.PG_CONN_TIMEOUT || 5000),
  };
  if (String(process.env.PGSSLMODE || '').toLowerCase() === 'require') {
    config.ssl = { rejectUnauthorized: false };
  }
  const pool = new Pool(config);
  if (!process.env.SUPPRESS_DB_LOG) {
    console.log(`[DB] Connecting to ${config.user}@${config.host}:${config.port}/${config.database} (ssl=${config.ssl ? 'on' : 'off'})`);
  }
  return pool;
}

const pool = buildPool();

// Apply per-connection settings
pool.on('connect', (client) => {
  // 5s statement timeout; 10s idle in transaction timeout
  client.query(`SET statement_timeout TO ${Number(process.env.PG_STMT_TIMEOUT || 5000)};`).catch(()=>{});
  client.query(`SET idle_in_transaction_session_timeout TO ${Number(process.env.PG_IDLE_TX_TIMEOUT || 10000)};`).catch(()=>{});
});

module.exports = pool;