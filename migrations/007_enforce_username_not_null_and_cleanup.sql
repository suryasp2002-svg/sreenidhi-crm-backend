-- Migration: Enforce username NOT NULL, clean sample emails, and backfill usernames
-- Safe to re-run: guarded unique handling
BEGIN;

-- 1) Clear sample/demo emails
UPDATE public.users
   SET email = NULL
 WHERE LOWER(email) IN ('owner@example.com','emp1@example.com');

-- 2) Backfill username for any missing/blank usernames using email local-part when available
UPDATE public.users
   SET username = LOWER(split_part(email,'@',1))
 WHERE (username IS NULL OR btrim(username) = '')
   AND email IS NOT NULL AND btrim(email) <> '';

-- 3) For any still missing usernames, derive from full_name or fallback and append short id for uniqueness
UPDATE public.users u
   SET username = COALESCE(
                    NULLIF(LOWER(regexp_replace(COALESCE(full_name,''), '[^a-z0-9]+', '', 'g')), ''),
                    'user'
                  ) || '_' || substr(REPLACE(u.id::text,'-',''), 1, 6)
 WHERE (username IS NULL OR btrim(username) = '');

-- 4) Ensure uniqueness for any duplicates by appending a short hash (skip first occurrence)
WITH dups AS (
  SELECT username, array_agg(id ORDER BY id) AS ids
    FROM public.users
   GROUP BY username
  HAVING COUNT(*) > 1
)
UPDATE public.users u
   SET username = u.username || '_' || substr(md5(u.id::text), 1, 6)
  FROM dups
 WHERE u.username = dups.username
   AND u.id <> (dups.ids)[1];

-- 5) Enforce NOT NULL on username
ALTER TABLE public.users ALTER COLUMN username SET NOT NULL;

-- 6) Keep UNIQUE on username if not already present
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'public.users'::regclass
       AND contype = 'u'
       AND conname = 'users_username_key'
  ) THEN
    BEGIN
      ALTER TABLE public.users ADD CONSTRAINT users_username_key UNIQUE (username);
    EXCEPTION WHEN duplicate_table THEN
      -- ignore if concurrent or already there
      NULL;
    END;
  END IF;
END$$;

COMMIT;
