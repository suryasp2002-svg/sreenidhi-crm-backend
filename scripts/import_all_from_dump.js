#!/usr/bin/env node
/**
 * Import ALL tables from a pg_dump text file into the current database by streaming COPY blocks.
 * - Skips DDL (DROP/CREATE) and only replays COPY data and SEQUENCE SET.
 * - Disables constraints (session_replication_role=replica) during load, then restores.
 * - Reads the dump as UTF-8 text but strips all NUL (0x00) bytes which can appear in some exports.
 * - Streams raw COPY data to the server using COPY FROM STDIN to preserve escapes and binary bytea.
 *
 * Usage:
 *   node scripts/import_all_from_dump.js <path-to-dump.sql> [DATABASE_URL]
 */
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const copyFrom = require('pg-copy-streams').from;

const dumpPath = process.argv[2] || path.resolve(__dirname, '..', '..', 'postgresql-project', 'crm_backup.sql');
const connectionString = process.argv[3] || process.env.DATABASE_URL;

if (!fs.existsSync(dumpPath)) {
  console.error('Dump file not found:', dumpPath);
  process.exit(2);
}
if (!connectionString) {
  console.error('DATABASE_URL missing. Pass as 2nd arg or set env.');
  process.exit(2);
}

const rawBuffer = fs.readFileSync(dumpPath);
// Strip all NUL bytes which break JS string handling and are not valid in COPY text segments
const filteredBuffer = Buffer.from([...rawBuffer].filter(b => b !== 0x00));
const rawText = filteredBuffer.toString('utf8');

// Pre-extract DDL we may need for legacy tables not present in current schema
function extractCreateTables(text) {
  const out = new Map();
  const re = /CREATE TABLE public\.([a-zA-Z0-9_]+)\s*\(([\s\S]*?)\);[\r\n]+/gm;
  let m;
  while ((m = re.exec(text)) !== null) {
    const name = m[1];
    const body = m[2];
    const sql = `CREATE TABLE public.${name} (\n${body}\n);`;
    out.set(name, sql);
  }
  return out;
}

function extractIdDefaultAlters(text) {
  const out = [];
  const re = /ALTER TABLE ONLY public\.([a-zA-Z0-9_]+) ALTER COLUMN id SET DEFAULT nextval\('public\.([a-zA-Z0-9_]+)'::regclass\);/gm;
  let m;
  while ((m = re.exec(text)) !== null) {
    out.push({ table: m[1], sequence: m[2] });
  }
  return out;
}

// Helper: find all COPY blocks
function* findCopyBlocks(text) {
  const copyRe = /COPY\s+public\.([a-zA-Z0-9_]+)\s*\(([^)]*)\)\s*FROM\s+stdin;[\r\n]+/gm;
  let m;
  while ((m = copyRe.exec(text)) !== null) {
    const table = m[1];
    const columns = m[2].trim();
    const start = copyRe.lastIndex; // position after the semicolon+newline
    // Find the end marker for this block: a line with just "\\."
    const endRe = /(\r?\n)\\\.(\r?\n)/gm;
    endRe.lastIndex = start;
    const em = endRe.exec(text);
    // Also detect the next COPY header to handle dumps that omit explicit terminators in metadata
    const nextCopyRe = /COPY\s+public\./gm;
    nextCopyRe.lastIndex = start;
    const nm = nextCopyRe.exec(text);
    const nextCopyIdx = nm ? nm.index : Number.POSITIVE_INFINITY;

    if (!em || nextCopyIdx < em.index) {
      // Treat as empty data block; advance to next COPY header (or end of text)
      yield { table, columns, dataSlice: '' };
      copyRe.lastIndex = nextCopyIdx; // continue scanning from the next COPY
    } else {
      const endIdx = em.index; // data ends right before the terminator sequence
      const dataSlice = text.slice(start, endIdx);
      yield { table, columns, dataSlice };
      // Move regex index forward to search after this block's terminator
      copyRe.lastIndex = endRe.lastIndex; // skip past the terminator
    }
  }
}

async function main() {
  const pool = new Pool({ connectionString, ssl: { rejectUnauthorized: false } });
  const client = await pool.connect();
  try {
    console.log('Starting import from dump:', dumpPath);
  try { await client.query("SET session_replication_role = 'replica'"); } catch (e) { console.warn('Note: could not disable constraints (non-superuser)'); }

    // Create legacy/missing tables if not present
    const tables = extractCreateTables(rawText);
    for (const [name, sql] of tables.entries()) {
      try {
        const exists = await client.query("SELECT to_regclass($1) AS r", [`public.${name}`]);
        if (!exists.rows[0].r) {
          console.log(`Creating missing table: ${name}`);
          await client.query(sql);
        }
      } catch (e) {
        console.warn(`Skipping table ${name} creation due to error: ${e.message}`);
      }
    }

    // Ensure id defaults via sequences where specified
    const alters = extractIdDefaultAlters(rawText);
    for (const { table, sequence } of alters) {
      try {
        const tbl = await client.query("SELECT to_regclass($1) AS r", [`public.${table}`]);
        if (!tbl.rows[0].r) continue; // skip if table actually doesn't exist
        // Create sequence if missing
        const seq = await client.query("SELECT to_regclass($1) AS r", [sequence.startsWith('public.') ? sequence : `public.${sequence}`]);
        const seqName = sequence.startsWith('public.') ? sequence : `public.${sequence}`;
        if (!seq.rows[0].r) {
          console.log(`Creating missing sequence: ${seqName}`);
          await client.query(`CREATE SEQUENCE IF NOT EXISTS ${seqName} START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;`);
        }
        // Apply default
        await client.query(`ALTER TABLE ONLY public.${table} ALTER COLUMN id SET DEFAULT nextval('${seqName}'::regclass);`);
      } catch (e) {
        console.warn(`Skipping default for ${table}.id due to error: ${e.message}`);
      }
    }

    // Stream each COPY block
    let count = 0;
    for (const { table, columns, dataSlice } of findCopyBlocks(rawText)) {
      count++;
      try {
        const exists = await client.query("SELECT to_regclass($1) AS r", [`public.${table}`]);
        if (!exists.rows[0].r) {
          console.warn(`Skipping COPY for ${table}: table not found`);
          continue;
        }
        console.log(`COPY -> ${table} (${columns.split(',').length} cols)`);
        const copySql = `COPY public.${table} (${columns}) FROM STDIN`;
        const stream = client.query(copyFrom(copySql));
        const needsNewline = dataSlice.length && !dataSlice.endsWith('\n');
        stream.write(Buffer.from(dataSlice + (needsNewline ? '\n' : ''), 'utf8'));
        stream.end();
        await new Promise((resolve, reject) => {
          stream.on('finish', resolve);
          stream.on('error', reject);
        });
      } catch (e) {
        console.warn(`COPY failed for ${table}: ${e.message}`);
      }
    }
    console.log(`Replayed ${count} COPY block(s).`);

    // Apply SEQUENCE SET values
    const seqRe = /SELECT\s+pg_catalog\.setval\('([^']+)',\s*(\d+),\s*(true|false)\);/gm;
    let sm;
    let seqCount = 0;
    while ((sm = seqRe.exec(rawText)) !== null) {
      const seqName = sm[1];
      const val = Number(sm[2]);
      const isCalled = sm[3] === 'true';
      await client.query('SELECT pg_catalog.setval($1,$2,$3);', [seqName, val, isCalled]);
      seqCount++;
    }
    console.log(`Applied ${seqCount} sequence set(s).`);

  try { await client.query("SET session_replication_role = 'origin'"); } catch {}
    console.log('Import complete.');
  } catch (err) {
    console.error('Import failed:', err.message);
    process.exit(1);
  } finally {
    client.release();
    await pool.end();
  }
}

main();
