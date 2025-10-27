// Calls the /api/email/preview/meeting endpoint with a sample payload and prints the response JSON
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
    const payload = {
      title: 'Site Visit',
      clientName: 'ACME Corp',
      personName: 'Sanjay',
      startsAt: new Date(Date.now() + 2*60*60*1000).toISOString().slice(0,19),
      endsAt: new Date(Date.now() + 3*60*60*1000).toISOString().slice(0,19),
      location: 'ACME Campus, Hyderabad',
      meetingLink: 'https://meet.google.com/abc-defg-hij'
    };
    const body = JSON.stringify(payload);
    const opts = {
      hostname: 'localhost',
      port: 5000,
      path: '/api/email/preview/meeting',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
        'Authorization': `Bearer ${token}`
      }
    };
    await new Promise((resolve, reject) => {
      const req = http.request(opts, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
          try {
            const o = JSON.parse(data);
            console.log(JSON.stringify({ statusCode: res.statusCode, ok: res.statusCode >= 200 && res.statusCode < 300, subject: o.subject, icsUrl: o.icsUrl, googleUrl: o.googleUrl, htmlLength: (o.html || '').length }, null, 2));
            resolve();
          } catch (e) {
            console.error('Non-JSON response:', data);
            reject(e);
          }
        });
      });
      req.on('error', reject);
      req.write(body);
      req.end();
    });
    process.exit(0);
  } catch (e) {
    console.error(e.message || e);
    process.exit(1);
  }
})();