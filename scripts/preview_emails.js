const { meetingEmailHtml } = require('../utils/templates/meetingEmail');
const { remindersEmailHtml } = require('../utils/templates/remindersEmail');
const fs = require('fs');
const path = require('path');

function write(file, html) {
  fs.writeFileSync(file, html, 'utf8');
  console.log('Wrote', file);
}

function sampleMeeting() {
  return {
    id: 'sample-meeting-1',
    title: 'Week-1 payment discussion',
    clientName: 'siddu',
    personName: 'kiran',
    // Provide actual Date objects for calendar link generation
    dateText: 'Fri, 21 Nov, 2025',
    timeText: '11:00 am – 12:00 pm',
    startsAt: new Date(Date.now() + 24 * 3600 * 1000),
    endsAt: new Date(Date.now() + 25 * 3600 * 1000),
    location: 'Office (Hyderabad)',
    meetingLink: 'https://example.com/meet/abc'
  };
}

function sampleReminders() {
  return {
    items: [
      { kind: 'EMAIL', client_name: 'siddu', title: 'Send invoice', when: new Date(), person_name: 'kiran', email: 'siddu@example.com', notes: 'Attach last statement.' },
      { kind: 'CALL', client_name: 'siddu', title: 'Follow-up call', when: new Date(Date.now()+3600*1000), person_name: 'kiran', phone: '+91-99999-00000', notes: 'Confirm payment schedule.' }
    ]
  };
}

function main() {
  const outDir = path.join(__dirname, '../tmp_preview');
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir);
  write(path.join(outDir, 'meeting.html'), meetingEmailHtml(sampleMeeting()));
  write(path.join(outDir, 'reminders.html'), remindersEmailHtml(sampleReminders()));
  console.log('\nPreview complete. Open the generated HTML files in a browser to inspect the logo rendering.');
  console.log('If the logo is missing, set EMAIL_LOGO_PATH or EMAIL_LOGO_BASE64 env vars or place an image at server/assets/logo.png');
}

main();
