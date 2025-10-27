// Attempts to send a test email via /api/email/send and prints the response
process.env.SUPPRESS_DB_LOG = '1';
const http = require('http');
const jwt = require('jsonwebtoken');
const pool = require('../db');

(async () => {
  try {
    const r = await pool.query("SELECT id, email, username, full_name, role FROM public.users WHERE active=TRUE ORDER BY (role='OWNER') DESC, created_at ASC LIMIT 1");
    if (!r.rows.length) throw new Error('No active users found');
    const user = r.rows[0];
    const JWT_SECRET = process.env.JWT_SECRET || 'dev_secret_change_me';
    const token = jwt.sign({ sub: user.id, role: user.role, email: user.email, username: user.username || undefined }, JWT_SECRET, { expiresIn: '15m' });
    const meeting = {
      title: 'Site Visit',
      clientName: 'ACME Corp',
      personName: 'Sanjay',
      startsAt: new Date(Date.now() + 2*60*60*1000).toISOString().slice(0,19),
      endsAt: new Date(Date.now() + 3*60*60*1000).toISOString().slice(0,19),
      location: 'ACME Campus, Hyderabad',
      meetingLink: 'https://meet.google.com/abc-defg-hij'
    };
    const bodyObj = {
      to: user.email || 'owner@example.com',
      subject: 'Test Email (CRM) — ' + new Date().toLocaleString('en-IN'),
      html: '<p>This is a test email from the CRM server.</p>',
      meeting
    };
    const body = JSON.stringify(bodyObj);
    const opts = {
      hostname: 'localhost',
      port: 5000,
      path: '/api/email/send',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
        'Authorization': `Bearer ${token}`
      }
    };
    await new Promise((resolve) => {
      const req = http.request(opts, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
          let parsed;
          try { parsed = JSON.parse(data); } catch { parsed = { raw: data }; }
          console.log(JSON.stringify({ statusCode: res.statusCode, response: parsed }, null, 2));
          resolve();
        });
      });
      req.on('error', (e) => {
        console.error('Request error:', e.message);
        console.log(JSON.stringify({ statusCode: 0, error: e.message }, null, 2));
        resolve();
      });
      req.write(body);
      req.end();
    });
    process.exit(0);
  } catch (e) {
    console.error(e.message || e);
    process.exit(1);
  }
})();