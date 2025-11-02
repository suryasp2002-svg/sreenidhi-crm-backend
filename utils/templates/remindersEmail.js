function escapeHtml(s = '') {
  return String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

function fmtLocal(date) {
  try {
    const d = (date instanceof Date) ? date : new Date(date);
    if (isNaN(d.getTime())) return { dateText: '-', timeText: '-' };
    const dateText = d.toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: 'numeric' }).replace(/\./g, '');
    const timeText = d.toLocaleTimeString('en-IN', { hour: 'numeric', minute: '2-digit' });
    return { dateText, timeText };
  } catch { return { dateText: '-', timeText: '-' }; }
}
function reminderCard(rem, idx) {
  const { dateText, timeText } = fmtLocal(rem.when);
  const type = String(rem.kind || rem.type || 'REMINDER').toUpperCase();
  // Base fields common to both types
  const fields = [
    ['Type', type === 'CALL' ? 'Call' : 'Email'],
    ['Client', rem.client_name || '-'],
    ['Title', rem.title || '-'],
    ['Date', dateText],
    ['Time', timeText],
    ['Person', rem.person_name || '-'],
  ];
  // Type-specific field: show Phone Number for CALL, Email ID for EMAIL
  if (type === 'CALL') {
    fields.push(['Phone Number', rem.phone || '-']);
  } else {
    fields.push(['Email ID', rem.receiver_email || rem.email || '-']);
  }
  // Notes (optional)
  fields.push(['Notes', rem.notes || '-']);

  return `
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:separate;margin-top:14px;">
      <tr>
        <td style="font-weight:800;font-size:18px;color:#111;padding:4px 2px 8px">Reminder ${idx + 1}</td>
      </tr>
      <tr>
        <td style="border:1px solid #e5e7eb;border-radius:12px;padding:14px 16px;background:#fff;">
          <table role="presentation" width="100%" cellspacing="0" cellpadding="0">
            ${fields.map(([label, value]) => `
              <tr>
                <td style="width:140px;color:#6b7280;padding:6px 6px 6px 0;vertical-align:top;">${escapeHtml(label)}:</td>
                <td style="color:#111;padding:6px 0">${escapeHtml(String(value))}</td>
              </tr>
            `).join('')}
          </table>
        </td>
      </tr>
    </table>`;
}

function remindersEmailHtml(payload) {
  // payload: { items: Array<...>, calendar?: { googleUrl, appleUrl, outlookUrl, teamsUrl, icsUrl } }
  const items = Array.isArray(payload?.items) ? payload.items : [];
  const calendar = payload && payload.calendar || {};
  const isProd = (process.env.NODE_ENV || '').toLowerCase() === 'production';
  const ui = process.env.SITE_ORIGIN || process.env.FRONTEND_ORIGIN || (!isProd ? 'http://localhost:3000' : '');
  const logoUrl = process.env.EMAIL_LOGO_URL
    || (ui ? `${ui.replace(/\/$/, '')}/assets/branding/logo.png` : '/assets/branding/logo.png');

  const buttons = `
    <div style="display:flex;flex-wrap:wrap;gap:12px;margin-top:18px;">
      ${calendar.googleUrl ? `<a href="${calendar.googleUrl}" style="background:#1a73e8;color:#fff;padding:10px 14px;border-radius:10px;text-decoration:none;font-weight:700;display:inline-block;">Google Calendar</a>` : ''}
      ${calendar.appleUrl ? `<a href="${calendar.appleUrl}" style="background:#111827;color:#fff;padding:10px 14px;border-radius:10px;text-decoration:none;font-weight:700;display:inline-block;">Apple Calendar</a>` : ''}
      ${calendar.teamsUrl ? `<a href="${calendar.teamsUrl}" style="background:#4f46e5;color:#fff;padding:10px 14px;border-radius:10px;text-decoration:none;font-weight:700;display:inline-block;">Teams Calendar</a>` : ''}
      ${calendar.outlookUrl ? `<a href="${calendar.outlookUrl}" style="background:#2563eb;color:#fff;padding:10px 14px;border-radius:10px;text-decoration:none;font-weight:700;display:inline-block;">Outlook Calendar</a>` : ''}
    </div>`;

  return `
  <div style="font-family:Inter,Segoe UI,Helvetica,Arial,sans-serif;background:#f6f7f9;padding:24px;">
    <table role="presentation" cellspacing="0" cellpadding="0" width="100%" style="max-width:720px;margin:0 auto;background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 10px 30px rgba(0,0,0,0.08);">
      <tr>
        <td style="background:#0f172a;padding:18px 22px;color:#fff;">
          <table role="presentation" width="100%" cellspacing="0" cellpadding="0">
            <tr>
              <td style="vertical-align:middle;width:48px;">
                ${logoUrl ? `<img src="${logoUrl}" alt="Logo" width="48" height="48" style="display:block;border-radius:50%;object-fit:cover" />` : `<div style=\"background:#ffd54d;color:#111;width:48px;height:48px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-weight:800;\">SF</div>`}
              </td>
              <td style="vertical-align:middle;">
                <div style="font-size:20px;font-weight:800;letter-spacing:.3px">Sreenidhi Fuels</div>
                <div style="font-size:13px;opacity:.9;margin-top:2px">Reminder Notification</div>
              </td>
            </tr>
          </table>
        </td>
      </tr>
      <tr>
        <td style="padding:20px 22px;">
          ${items.map((it, idx) => reminderCard(it, idx)).join('')}
          ${buttons}
          <div style="margin-top:8px;color:#6b7280;font-size:12px;">Tip: On iPhone, tapping Apple Calendar will download an .ics file. Open it to add all reminders.</div>
          <div style="margin-top:18px;color:#6b7280;font-size:12px;text-align:center;">This is an auto email for reminders. Do not reply.</div>
        </td>
      </tr>
    </table>
    <div style="display:none;max-height:0;overflow:hidden">
      <style type="text/css">
        @media only screen and (max-width: 520px) {
          table[role='presentation']{width:100%!important}
          td{padding-left:16px!important;padding-right:16px!important}
        }
      </style>
    </div>
  </div>`;
}

module.exports = { remindersEmailHtml };
