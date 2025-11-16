const pool = require('../db');
(async () => {
  const client = await pool.connect();
  try {
    const unitType = 'TRUCK';
    const loadType = 'PURCHASE';
    const limit = 50;
    const sql = `SELECT fl.id, fl.unit_id, COALESCE(fl.load_date, fl.loaded_date) AS load_date,
                        fl.loaded_liters, fl.used_liters, fl.stock_status,
                        fl.lot_code_created, fl.created_at, fl.load_time, fl.load_type, su.unit_code, su.unit_type
                   FROM public.fuel_lots fl
                   JOIN public.storage_units su ON su.id = fl.unit_id
                  WHERE su.active=TRUE AND su.unit_type=$1 AND fl.load_type=$2
                  ORDER BY COALESCE(fl.load_time, fl.created_at) DESC, fl.id DESC
                  LIMIT ${limit}`;
    const r = await client.query(sql, [unitType, loadType]);
    console.log('[probe] rows:', r.rows.length);
    console.log(r.rows.slice(0,5));
  } catch (e) {
    console.error('probe failed:', e);
  } finally {
    client.release();
    process.exit(0);
  }
})();
