-- Adds an optional meeting_link column for online meeting URL
ALTER TABLE public.meetings ADD COLUMN IF NOT EXISTS meeting_link TEXT NULL;