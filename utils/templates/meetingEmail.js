const { generateGoogleCalendarLink, generateOutlookCalendarLink, generateTeamsLink } = require('../calendar');
const fs = require('fs');
const path = require('path');

function escapeHtml(s = '') {
  return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;','\'':'&#39;'}[c]));
}

// Remove any http/https URLs from a text block to avoid auto-linking by email clients
function stripUrls(text = '') {
  return String(text)
    .replace(/https?:\/\/[^\s)]+/gi, '')
    .replace(/\s{2,}/g, ' ')
    .trim();
}

function resolveInlineLogo() {
  const envB64 = process.env.EMAIL_LOGO_BASE64 && process.env.EMAIL_LOGO_BASE64.trim();
  if (envB64) return `data:image/png;base64,${envB64}`;
  const filePath = process.env.EMAIL_LOGO_PATH || path.join(__dirname, '../../assets/branding/logo.png');
  try {
    if (fs.existsSync(filePath)) {
      const buf = fs.readFileSync(filePath);
      return `data:image/png;base64,${buf.toString('base64')}`;
    }
  } catch {}
  return null;
}

function meetingEmailHtml(meeting) {
  const { id, title, clientName, personName, dateText, timeText, location, meetingLink } = meeting;
  const googleUrl = generateGoogleCalendarLink(meeting);
  const outlookUrl = generateOutlookCalendarLink(meeting);
  const teamsUrl = generateTeamsLink(meeting);
  const api = process.env.API_ORIGIN || '';
  const isProd = (process.env.NODE_ENV || '').toLowerCase() === 'production';
  // Prefer an explicit email-safe absolute URL; else try site origin; else dev localhost; else relative for preview
  // Prefer explicit SITE/FRONTEND origin. In production, if not provided, fall back to the deployed frontend URL.
  const ui = process.env.SITE_ORIGIN || process.env.FRONTEND_ORIGIN || (!isProd ? 'http://localhost:3000' : 'https://sreenidhi-crm-frontend.vercel.app');
  // Build a robust absolute logo URL: prefer explicit EMAIL_LOGO_URL, else UI origin, else API origin host, else localhost.
  const originFallback = ui || (api ? api.replace(/\/$/, '') : 'http://localhost:3000');
  const versionTag = process.env.EMAIL_LOGO_VERSION ? `?v=${encodeURIComponent(process.env.EMAIL_LOGO_VERSION)}` : '';
  const baseLogo = (process.env.EMAIL_LOGO_URL && process.env.EMAIL_LOGO_URL.trim())
    ? process.env.EMAIL_LOGO_URL.trim()
    : `${originFallback.replace(/\/$/, '')}/assets/branding/logo.png`;
  const logoUrl = `${baseLogo}${versionTag}`;
  const inlineLogo = resolveInlineLogo();
  const finalLogo = inlineLogo || logoUrl;
  const icsUrl = `${api}/api/meetings/${encodeURIComponent(id)}/ics`;
  // Prefer webcal:// scheme for Apple to open Calendar app on iOS when API_ORIGIN is a real host
  const preferWebcal = api && !/^(https?:\/\/)?(localhost|127\.0\.0\.1)(:|$)/i.test(api);
  const appleUrl = preferWebcal ? icsUrl.replace(/^https?:/i, 'webcal:') : icsUrl;
  // Location should be plain text without clickable links
  const safeLocation = location ? stripUrls(location) : '';
  return `
  <div style="font-family:Inter,Segoe UI,Helvetica,Arial,sans-serif;background:#f6f7f9;padding:24px;">
    <table role="presentation" cellspacing="0" cellpadding="0" width="100%" style="max-width:640px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 10px 30px rgba(0,0,0,0.06);">
      <tr>
        <td style="background:#d62839;padding:20px 24px;color:#fff;">
          <div style="display:flex;align-items:center;gap:12px;">
            ${finalLogo ? `<img src="${finalLogo}" alt="Sreenidhi Fuels Logo" width="96" style="display:block;height:96px;width:96px;object-fit:contain;border-radius:50%;flex-shrink:0;" />` : `<div style=\"background:#ffd54d;color:#111;width:96px;height:96px;border-radius:12px;display:flex;align-items:center;justify-content:center;font-weight:800;font-size:28px;\">SF</div>`}
            <div>
              <div style="font-size:18px;font-weight:800;letter-spacing:.3px;">SREENIDHI</div>
              <div style="font-size:12px;opacity:.9;margin-top:-2px;">FUELS</div>
            </div>
          </div>
          <div style="margin-top:12px;font-size:15px;letter-spacing:.5px;">MEETING NOTIFICATION</div>
        </td>
      </tr>
      <tr>
        <td style="padding:24px;">
          <div style="display:flex;align-items:center;gap:10px;margin-bottom:6px;">
            <div style="font-size:22px;font-weight:800;">New Meeting Scheduled</div>
          </div>
          <p style="color:#4b5563;font-size:14px;margin:8px 0 16px;">A new meeting has been created in your CRM schedule. Below are the details:</p>
          <table role="presentation" cellspacing="0" cellpadding="0" width="100%" style="border-top:1px solid #eee;margin-top:8px;padding-top:12px;">
            <tr><td style="padding:6px 0;width:140px;color:#6b7280;">Client:</td><td style="padding:6px 0;color:#111;font-weight:600;">${escapeHtml(clientName)}</td></tr>
            <tr><td style="padding:6px 0;color:#6b7280;">Title:</td><td style="padding:6px 0;color:#111;">${escapeHtml(title)}</td></tr>
            ${personName ? `<tr><td style="padding:6px 0;color:#6b7280;">Person:</td><td style="padding:6px 0;color:#111;">${escapeHtml(personName)}</td></tr>` : ''}
            <tr><td style="padding:6px 0;color:#6b7280;">Date:</td><td style="padding:6px 0;color:#111;">${escapeHtml(dateText)}</td></tr>
            <tr><td style="padding:6px 0;color:#6b7280;">Time:</td><td style="padding:6px 0;color:#111;">${escapeHtml(timeText)}</td></tr>
            ${safeLocation ? `<tr><td style=\"padding:6px 0;color:#6b7280;\">Location:</td><td style=\"padding:6px 0;color:#111;\">${escapeHtml(safeLocation)}</td></tr>` : ''}
            ${meetingLink ? `<tr><td style=\"padding:6px 0;color:#6b7280;\">Meeting Link:</td><td style=\"padding:6px 0;\"><a href=\"${meetingLink}\" style=\"color:#dc2626;text-decoration:none;\">Join Meeting</a></td></tr>` : ''}
          </table>
          <div style="margin:18px 0 8px;height:1px;background:#eee;"></div>
          <div style="display:flex;flex-wrap:wrap;gap:10px;">
            <a href="${googleUrl}" style="background:#1a73e8;color:#fff;padding:10px 14px;border-radius:8px;text-decoration:none;font-weight:600;display:inline-block;">Add to Google Calendar</a>
            <a href="${appleUrl}" target="_blank" style="background:#111827;color:#fff;padding:10px 14px;border-radius:8px;text-decoration:none;font-weight:600;display:inline-block;">Add to Apple Calendar</a>
            <a href="${teamsUrl || icsUrl}" style="background:#4f46e5;color:#fff;padding:10px 14px;border-radius:8px;text-decoration:none;font-weight:600;">Add to Teams Calendar</a>
            <a href="${outlookUrl || icsUrl}" style="background:#1a73e8;color:#fff;padding:10px 14px;border-radius:8px;text-decoration:none;font-weight:600;">Add to Outlook Calendar</a>
          </div>
          <div style="margin-top:8px;color:#6b7280;font-size:12px;">If the Apple button doesn’t open your calendar in some email apps, <a href="${icsUrl}" style="color:#1a73e8;text-decoration:none;">download the ICS</a> instead.</div>
          <p style="color:#6b7280;font-size:12px;margin-top:18px;">This is an automated email — please do not reply.</p>
        </td>
      </tr>
    </table>
    <!-- simple responsive tweak for smaller screens -->
    <div style="display:none;max-height:0;overflow:hidden">
      <style type="text/css">
        @media only screen and (max-width: 480px) {
          table[role='presentation']{width:100%!important}
          td{padding-left:16px!important;padding-right:16px!important}
        }
      </style>
    </div>
  </div>`;
}

module.exports = { meetingEmailHtml };
