const conn = process.argv[2];
if (conn && !process.env.DATABASE_URL) process.env.DATABASE_URL = conn;
const pool = require('./db');
(async ()=>{
  try{
    const res = await pool.query("SELECT count(*) AS c FROM users WHERE email IS NULL");
    console.log('[check_null_emails] count null emails =', res.rows[0].c);
    const sample = await pool.query("SELECT id, username, email FROM users WHERE email IS NULL LIMIT 5");
    console.log('[check_null_emails] sample rows:', sample.rows);
    process.exit(0);
  }catch(e){console.error(e); process.exit(1);} 
})()
