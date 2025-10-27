const fs = require('fs');
const path = require('path');
const p = process.argv[2] || path.resolve(__dirname, '..', '..', 'postgresql-project', 'crm_backup.sql');
const raw = fs.readFileSync(p);
const buf = Buffer.from([...raw].filter(b => b !== 0x00));
const text = buf.toString('utf8');
const copyRe = /COPY\s+public\.([a-zA-Z0-9_]+)\s*\(([^)]*)\)\s*FROM\s+stdin;[\r\n]+/gm;
let m; let i=0;
while ((m = copyRe.exec(text)) !== null) {
  const table = m[1];
  const start = copyRe.lastIndex;
  const endRe = /(\r?\n)\\\.(\r?\n)/gm; endRe.lastIndex = start; const em = endRe.exec(text);
  const nextCopyRe = /COPY\s+public\./gm; nextCopyRe.lastIndex = start; const nm = nextCopyRe.exec(text); const nextIdx = nm ? nm.index : text.length;
  let dataSlice = '';
  if (!em || nextIdx < em.index) {
    dataSlice = text.slice(start, Math.min(start+200, nextIdx));
  } else {
    dataSlice = text.slice(start, Math.min(em.index, start+200));
  }
  console.log(++i, table, 'first200=', JSON.stringify(dataSlice));
}
