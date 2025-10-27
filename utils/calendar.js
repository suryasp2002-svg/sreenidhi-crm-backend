const { createEvent } = require('ics');

function toParts(d) {
  const dt = new Date(d);
  return [dt.getFullYear(), dt.getMonth() + 1, dt.getDate(), dt.getHours(), dt.getMinutes()];
}

function generateICS(meeting) {
  const { title, clientName, personName, startsAt, endsAt, location, meetingLink } = meeting;
  // Only include a URL if it looks valid; the ics lib validates strictly
  const isValidUrl = (u) => {
    if (!u || typeof u !== 'string') return false;
    try { const x = new URL(u); return !!(x.protocol && x.hostname); } catch { return false; }
  };
  return new Promise((resolve, reject) => {
    const base = {
      start: toParts(startsAt),
      end: toParts(endsAt),
      title: `${title} – ${clientName}`,
      description: `Client: ${clientName}\n${personName ? `Person: ${personName}\n` : ''}${isValidUrl(meetingLink) ? `Join: ${meetingLink}` : ''}`,
      location: location || '',
    };
    if (isValidUrl(meetingLink)) base.url = meetingLink;
    createEvent(base, (err, value) => {
      if (err) return reject(err);
      resolve(value);
    });
  });
}

function googleDate(d) {
  return new Date(d).toISOString().replace(/[-:]/g, '').replace(/\.\d{3}Z$/, 'Z');
}

function generateGoogleCalendarLink(meeting) {
  const { title, clientName, startsAt, endsAt, location, meetingLink } = meeting;
  const dates = `${googleDate(startsAt)}/${googleDate(endsAt)}`;
  const params = new URLSearchParams({
    action: 'TEMPLATE',
    text: `${title} – ${clientName}`,
    details: meetingLink ? `Join: ${meetingLink}` : '',
    location: location || '',
    dates,
  });
  return `https://www.google.com/calendar/render?${params.toString()}`;
}
// Outlook on the web (consumer). Office 365 portals typically redirect.
function generateOutlookCalendarLink(meeting) {
  const { title, clientName, startsAt, endsAt, location, meetingLink } = meeting;
  const params = new URLSearchParams({
    path: '/calendar/action/compose',
    rru: 'addevent',
    subject: `${title} – ${clientName}`,
    startdt: new Date(startsAt).toISOString(),
    enddt: new Date(endsAt).toISOString(),
    body: meetingLink ? `Join: ${meetingLink}` : '',
    location: location || ''
  });
  return `https://outlook.live.com/calendar/0/deeplink/compose?${params.toString()}`;
}

// Microsoft Teams: if a Teams meeting link is provided, prefer it; else open new meeting compose with details
function isTeamsUrl(u) {
  if (!u || typeof u !== 'string') return false;
  return /https?:\/\/teams\.microsoft\.com\//i.test(u);
}

function generateTeamsLink(meeting) {
  const { title, clientName, startsAt, endsAt, location, meetingLink } = meeting;
  if (isTeamsUrl(meetingLink)) return meetingLink;
  const params = new URLSearchParams({
    subject: `${title} – ${clientName}`,
    startTime: new Date(startsAt).toISOString(),
    endTime: new Date(endsAt).toISOString(),
    content: meetingLink ? `Join: ${meetingLink}` : '',
    location: location || ''
  });
  // Note: Teams deep link support varies; this opens a compose screen in most tenants
  return `https://teams.microsoft.com/l/meeting/new?${params.toString()}`;
}

module.exports = { generateICS, generateGoogleCalendarLink, generateOutlookCalendarLink, generateTeamsLink };
