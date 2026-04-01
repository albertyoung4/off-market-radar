/**
 * Sync HC Registry data from dbreader → Supabase for matched deal properties.
 *
 * Pulls attom_ids from matched/confirmed deals in Supabase,
 * queries dbo.hc_registry via property_registry join,
 * then upserts into Supabase hc_property_data table.
 *
 * Usage: node sync-hc-data.js
 *
 * Env vars (or uses defaults from marketplace-viewer/.env):
 *   DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
 *   SUPABASE_URL, SUPABASE_SERVICE_KEY
 */

require('dotenv').config({ path: require('path').join(__dirname, '../marketplace-viewer/.env') });

const { Pool } = require('pg');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

const pool = new Pool({
  host: process.env.DB_HOST,
  port: parseInt(process.env.DB_PORT, 10),
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  max: 5,
  connectionTimeoutMillis: 30000,
});

async function supabaseRequest(path, { method = 'GET', body, headers = {} } = {}) {
  const url = `${SUPABASE_URL}/rest/v1/${path}`;
  const resp = await fetch(url, {
    method,
    headers: {
      'apikey': SUPABASE_SERVICE_KEY,
      'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`,
      'Content-Type': 'application/json',
      'Prefer': method === 'POST' ? 'resolution=merge-duplicates' : '',
      ...headers,
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!resp.ok) {
    const err = await resp.text();
    throw new Error(`Supabase ${method} ${path}: ${resp.status} ${err}`);
  }
  const text = await resp.text();
  return text ? JSON.parse(text) : null;
}

async function getMatchedAttomIds() {
  // Fetch all matched/confirmed deals and extract attom_ids from match_candidates
  let allDeals = [];
  let offset = 0;
  const limit = 1000;

  while (true) {
    const deals = await supabaseRequest(
      `fb_deal_posts?select=match_candidates&match_status=in.(matched,confirmed)&match_confidence=eq.exact&limit=${limit}&offset=${offset}`
    );
    allDeals = allDeals.concat(deals);
    if (deals.length < limit) break;
    offset += limit;
  }

  const attomIds = new Set();
  for (const deal of allDeals) {
    const candidates = deal.match_candidates;
    if (Array.isArray(candidates) && candidates.length > 0) {
      const id = candidates[0]?.attom_id;
      if (id) attomIds.add(String(id));
    }
  }
  return [...attomIds];
}

async function fetchHcData(attomIds) {
  if (attomIds.length === 0) return [];

  // Query in batches of 500
  const batchSize = 500;
  const results = [];

  for (let i = 0; i < attomIds.length; i += batchSize) {
    const batch = attomIds.slice(i, i + batchSize);
    const placeholders = batch.map((_, idx) => `$${idx + 1}`).join(',');

    const rows = await pool.query(`
      SELECT pr.attom_id::text AS attom_id,
             hc.hc_value_estimate,
             hc.hc_rental_avm_lower,
             hc.hc_rental_avm_upper,
             hc.city,
             hc.state,
             hc.county
      FROM dbo.property_registry pr
      JOIN dbo.hc_registry hc ON hc.property_registry_id = pr.id
      WHERE pr.attom_id IN (${placeholders})
    `, batch);

    results.push(...rows.rows);
    if (i + batchSize < attomIds.length) {
      console.log(`  Fetched batch ${Math.floor(i / batchSize) + 1}...`);
    }
  }
  return results;
}

async function upsertToSupabase(rows) {
  if (rows.length === 0) return;

  // Deduplicate by attom_id (keep first occurrence)
  const seen = new Set();
  rows = rows.filter(r => {
    if (seen.has(r.attom_id)) return false;
    seen.add(r.attom_id);
    return true;
  });

  // Upsert in batches of 200
  const batchSize = 200;
  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize).map(r => ({
      attom_id: r.attom_id,
      hc_value_estimate: r.hc_value_estimate,
      hc_rental_avm_lower: r.hc_rental_avm_lower,
      hc_rental_avm_upper: r.hc_rental_avm_upper,
      city: r.city,
      state: r.state,
      county: r.county,
      synced_at: new Date().toISOString(),
    }));

    await supabaseRequest('hc_property_data', {
      method: 'POST',
      body: batch,
    });
  }
}

async function main() {
  console.log('1. Fetching matched deal attom_ids from Supabase...');
  const attomIds = await getMatchedAttomIds();
  console.log(`   Found ${attomIds.length} unique attom_ids from exact-match deals`);

  if (attomIds.length === 0) {
    console.log('   No attom_ids to sync. Done.');
    return;
  }

  console.log('2. Querying HC registry from dbreader...');
  const hcRows = await fetchHcData(attomIds);
  console.log(`   Got HC data for ${hcRows.length} / ${attomIds.length} properties`);

  console.log('3. Upserting to Supabase hc_property_data...');
  await upsertToSupabase(hcRows);
  console.log(`   Upserted ${hcRows.length} rows`);

  console.log('Done!');
  pool.end();
}

main().catch(err => {
  console.error('Sync failed:', err);
  pool.end();
  process.exit(1);
});
