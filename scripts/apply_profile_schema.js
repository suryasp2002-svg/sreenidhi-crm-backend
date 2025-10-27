const pool = require('../db');

async function run() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // users: joining_date and status
    await client.query(`ALTER TABLE public.users ADD COLUMN IF NOT EXISTS joining_date DATE DEFAULT CURRENT_DATE`);
    await client.query(`ALTER TABLE public.users ADD COLUMN IF NOT EXISTS status TEXT`);
    // add check constraint if missing
    const ck = await client.query(`SELECT 1 FROM pg_constraint WHERE conname='users_status_check'`);
    if (ck.rows.length === 0) {
      await client.query(`ALTER TABLE public.users ADD CONSTRAINT users_status_check CHECK (status IS NULL OR status IN ('ACTIVE','INACTIVE','ON_LEAVE','SUSPENDED'))`);
    }
    await client.query(`ALTER TABLE public.users ALTER COLUMN status SET DEFAULT 'ACTIVE'`);

    // user_profiles
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.user_profiles (
        user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
        date_of_birth DATE,
        gender TEXT CHECK (gender IN ('MALE','FEMALE','OTHER','PREFER_NOT_TO_SAY')),
        emergency_contact_name TEXT,
        emergency_contact_phone TEXT,
        address TEXT,
        pan TEXT,
        pan_normalized TEXT,
        aadhaar TEXT,
        aadhaar_last4 TEXT,
        updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
      )
    `);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_user_profiles_pan_norm ON public.user_profiles ((COALESCE(pan_normalized, '')))`);
    await client.query(`
      CREATE OR REPLACE FUNCTION touch_user_profiles() RETURNS trigger AS $$
      BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
      END; $$ LANGUAGE plpgsql;
    `);
    const trg = await client.query(`SELECT 1 FROM pg_trigger WHERE tgname='trg_touch_user_profiles'`);
    if (trg.rows.length === 0) {
      await client.query(`CREATE TRIGGER trg_touch_user_profiles BEFORE UPDATE ON public.user_profiles FOR EACH ROW EXECUTE FUNCTION touch_user_profiles()`);
    }

    // user_photos
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.user_photos (
        id BIGSERIAL PRIMARY KEY,
        user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        mime_type TEXT NOT NULL,
        file_name TEXT,
        file_size_bytes INTEGER NOT NULL,
        data BYTEA NOT NULL,
        created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
        CONSTRAINT chk_user_photo_size CHECK (file_size_bytes >= 0 AND file_size_bytes <= 5*1024*1024)
      )
    `);
    await client.query(`CREATE UNIQUE INDEX IF NOT EXISTS uniq_user_photos_user ON public.user_photos(user_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_user_photos_created_at ON public.user_photos(created_at DESC)`);

    // Convenience view for pgAdmin: combine users + user_profiles for full field visibility
    await client.query(`
      CREATE OR REPLACE VIEW public.user_full_profiles AS
      SELECT 
        u.id AS user_id,
        u.full_name,
        u.username,
        u.email,
        u.phone,
        u.role,
        u.joining_date,
        u.status,
        p.date_of_birth,
        p.gender,
        p.emergency_contact_name,
        p.emergency_contact_phone,
        p.address,
        p.pan,
        p.pan_normalized,
        p.aadhaar,
        p.aadhaar_last4,
        p.updated_at
      FROM public.users u
      LEFT JOIN public.user_profiles p ON p.user_id = u.id;
    `);

    await client.query('COMMIT');
    console.log('Profile schema applied.');
    process.exit(0);
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('Apply failed:', e.message);
    process.exit(1);
  } finally {
    client.release();
    await pool.end();
  }
}

run();
