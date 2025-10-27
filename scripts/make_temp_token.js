// Generates a temporary JWT for the first active user to exercise protected APIs
const jwt = require('jsonwebtoken');
const pool = require('../db');

(async () => {
  try {
    const r = await pool.query("SELECT id, email, username, full_name, role FROM public.users WHERE active=TRUE ORDER BY (role='OWNER') DESC, created_at ASC LIMIT 1");
    if (!r.rows.length) {
      console.error('No active users found');
      process.exit(2);
    }
    const user = r.rows[0];
    const JWT_SECRET = process.env.JWT_SECRET || 'dev_secret_change_me';
    const token = jwt.sign({ sub: user.id, role: user.role, email: user.email, username: user.username || undefined, full_name: user.full_name || undefined }, JWT_SECRET, { expiresIn: '1h' });
    console.log(JSON.stringify({ token, user }));
    process.exit(0);
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
})();