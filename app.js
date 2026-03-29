// ===== CONFIG (loaded from localStorage, with defaults) =====
const SUPABASE_URL = localStorage.getItem('omr_supabase_url') || 'https://xpvvgecwajqmveuuhnmc.supabase.co';
const SUPABASE_ANON_KEY = localStorage.getItem('omr_supabase_key') || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InhwdnZnZWN3YWpxbXZldXVobm1jIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQ0MzgzMTksImV4cCI6MjA5MDAxNDMxOX0.l6KhvXHp3WdltKYtiSfrAHAgRwRxtfh6lZ0B73i0myc';
const GMAPS_KEY = localStorage.getItem('omr_gmaps_key') || '';

const PAGE_SIZE = 50;
let currentPage = 0;
let currentFilter = 'matches';
let allDeals = [];
let totalCount = 0;
let currentView = 'dashboard';

// ===== DEDUPLICATION & PROPERTY DATA HELPERS =====
// Deduplicate deals by matched_address, keeping the first (most recent) occurrence
function deduplicateDeals(deals) {
  const seen = new Set();
  return deals.filter(d => {
    const addr = (d.matched_address || '').toUpperCase().trim();
    if (!addr) return true; // keep unmatched deals as-is
    if (seen.has(addr)) return false;
    seen.add(addr);
    return true;
  });
}

// Build a normalized full address from property registry data
function normalizeAddress(deal) {
  const c = deal.match_candidates?.[0];
  if (!c) return deal.matched_address || '';
  // Use registry street address, falling back to matched_address or parsed
  const street = c.property_address_full || deal.matched_address || deal.parsed_full_address || '';
  const city = c.property_address_city || deal.parsed_city || '';
  const state = c.property_address_state || deal.parsed_state || '';
  const zip = c.property_address_zip || deal.parsed_zip || '';
  // Build normalized: "123 Main St, Nashville, TN 37201"
  const parts = [street.trim()];
  if (city) parts.push(city.trim());
  const stateZip = [state.trim(), zip.trim()].filter(Boolean).join(' ');
  if (stateZip) parts.push(stateZip);
  return parts.filter(Boolean).join(', ');
}

// Backfill beds/baths/sqft and normalize address from match_candidates
function backfillPropertyData(deal) {
  const c = deal.match_candidates?.[0];
  if (!c) return deal;
  const normalized = normalizeAddress(deal);
  return {
    ...deal,
    matched_address: normalized || deal.matched_address,
    parsed_city: c.property_address_city || deal.parsed_city || null,
    parsed_state: c.property_address_state || deal.parsed_state || null,
    parsed_zip: c.property_address_zip || deal.parsed_zip || null,
    parsed_beds: deal.parsed_beds || c.bedrooms_count || null,
    parsed_baths: deal.parsed_baths || c.bath_count || null,
    parsed_sqft: deal.parsed_sqft || c.area_building || c.living_area_size || null,
  };
}

// Apply both: backfill then dedup
function cleanDeals(deals) {
  return deduplicateDeals(deals.map(backfillPropertyData));
}

// Safe base64 encoding that handles Unicode characters
function safeBtoa(str) {
  try {
    return btoa(str);
  } catch {
    return btoa(encodeURIComponent(str));
  }
}

// Clean poster names from scraper artifacts (e.g. "Name · Follow", "Name  · Follow")
function cleanPosterName(name) {
  return name.replace(/\s*[·•]\s*Follow\s*$/i, '').replace(/\s{2,}/g, ' ').trim();
}

// ===== NAVIGATION =====
function switchView(view) {
  currentView = view;

  // Update nav items
  document.querySelectorAll('.nav-item[data-view]').forEach(el => {
    el.classList.toggle('active', el.dataset.view === view);
  });

  // Update views
  document.querySelectorAll('.view').forEach(el => {
    el.classList.toggle('active', el.id === `view-${view}`);
  });

  // Update header title
  const titles = {
    dashboard: 'Dashboard',
    deals: 'Deal Feed',
    flow: 'Deal Flow Analytics',
    markets: 'Market Analytics',
    wholesalers: 'Wholesaler Directory',
    sources: 'Deal Source Catalog',
    team: 'Team Activity',
  };
  const headerEl = document.getElementById('headerTitle');
  if (view === 'dashboard') {
    headerEl.textContent = 'Leveraging AI to create the most comprehensive insight into off market deal flow';
    headerEl.classList.add('tagline');
  } else {
    headerEl.textContent = titles[view] || 'Dashboard';
    headerEl.classList.remove('tagline');
  }

  // Load data for view if needed
  if (view === 'deals') {
    loadDeals();
  }
  if (view === 'wholesalers') {
    loadWholesalers();
  }
  if (view === 'sources') {
    loadSources();
  }
  if (view === 'flow') {
    loadDealFlow();
  }
  if (view === 'markets') {
    loadMarkets();
  }
}

function refreshData() {
  loadKPIs();
  loadDashboardDeals();
  loadDashMap();
  if (currentView === 'deals') {
    loadDeals();
  }
}

// ===== SUPABASE REST HELPERS =====
async function supabaseGet(table, { select = '*', filters = [], order, limit, offset } = {}) {
  const url = new URL(`${SUPABASE_URL}/rest/v1/${table}`);
  url.searchParams.set('select', select);
  for (const f of filters) {
    url.searchParams.set(f.col, f.val);
  }
  if (order) url.searchParams.set('order', order);
  if (limit != null) url.searchParams.set('limit', limit);
  if (offset != null) url.searchParams.set('offset', offset);

  const resp = await fetch(url, {
    headers: {
      'apikey': SUPABASE_ANON_KEY,
      'Authorization': `Bearer ${SUPABASE_ANON_KEY}`,
      'Prefer': 'count=exact',
    },
  });

  if (!resp.ok) {
    const err = await resp.text();
    throw new Error(`Supabase error: ${resp.status} ${err}`);
  }

  const range = resp.headers.get('content-range');
  let count = 0;
  if (range) {
    const match = range.match(/\/(\d+)/);
    if (match) count = parseInt(match[1]);
  }

  const data = await resp.json();
  return { data, count };
}

// PATCH a single row by id
async function supabasePatch(table, id, updates) {
  const url = new URL(`${SUPABASE_URL}/rest/v1/${table}`);
  url.searchParams.set('id', `eq.${id}`);
  const resp = await fetch(url, {
    method: 'PATCH',
    headers: {
      'apikey': SUPABASE_ANON_KEY,
      'Authorization': `Bearer ${SUPABASE_ANON_KEY}`,
      'Content-Type': 'application/json',
      'Prefer': 'return=minimal',
    },
    body: JSON.stringify(updates),
  });
  if (!resp.ok) {
    const err = await resp.text();
    throw new Error(`Supabase PATCH error: ${resp.status} ${err}`);
  }
}

function statusFilters(filter) {
  if (filter === 'matches') {
    return [{ col: 'match_status', val: 'in.(matched,multi_match,confirmed)' }];
  }
  if (filter && filter !== 'all') {
    return [{ col: 'match_status', val: `eq.${filter}` }];
  }
  return [{ col: 'match_status', val: 'neq.pending' }];
}

// ===== DATA LOADING =====
async function loadDashboardDeals() {
  const filters = statusFilters('matches');
  try {
    const { data, count } = await supabaseGet('fb_deal_posts', {
      filters,
      order: 'captured_at.desc',
      limit: PAGE_SIZE,
      offset: 0,
    });

    allDeals = cleanDeals(data);
    totalCount = count;
    renderDealsToTable(allDeals, 'dashDealsBody');
    renderPaginationTo('dashPagination', count, 0);

    document.getElementById('dashLoadingState').style.display = 'none';
    document.getElementById('dashDealsTable').style.display = '';
    document.getElementById('dashDealCount').textContent = `${count} deal${count !== 1 ? 's' : ''}`;
    document.getElementById('navDealCount').textContent = count.toLocaleString();
  } catch (err) {
    console.error('Failed to load dashboard deals:', err);
    document.getElementById('dashLoadingState').innerHTML =
      `<div style="color:var(--red)">Failed to load deals: ${escapeHtml(err.message)}</div>`;
  }
}

// ===== DASHBOARD MAP =====
let dashMap = null;
let geoCache = JSON.parse(localStorage.getItem('omr_geocache') || '{}');

function saveGeoCache() {
  try { localStorage.setItem('omr_geocache', JSON.stringify(geoCache)); } catch(e) {}
}

async function loadDashMap() {
  const mapEl = document.getElementById('dashMap');
  if (!mapEl) return;

  // Initialize map if needed
  if (!dashMap) {
    dashMap = L.map('dashMap', {
      zoomControl: true,
      scrollWheelZoom: true,
      attributionControl: true,
    }).setView([36.5, -86.0], 6);

    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; OpenStreetMap',
      maxZoom: 18,
    }).addTo(dashMap);
  }

  try {
    // Fetch all matched/multi_match deals
    const { data } = await supabaseGet('fb_deal_posts', {
      select: 'id,match_status,matched_address,parsed_city,parsed_state,parsed_zip,parsed_asking_price,parsed_arv,parsed_beds,parsed_baths,parsed_sqft,match_candidates,match_confidence',
      filters: [{ col: 'match_status', val: 'in.(matched,multi_match)' }],
      limit: 1000,
    });

    // Deduplicate and backfill
    const cleaned = cleanDeals(data);
    // Build unique city list for geocoding
    const cityDeals = {};
    for (const deal of cleaned) {
      const c = deal.match_candidates?.[0];
      const city = c?.property_address_city || deal.parsed_city || '';
      const state = c?.property_address_state || deal.parsed_state || '';
      if (!city || !state) continue;
      const key = `${city}, ${state}`.toUpperCase();
      if (!cityDeals[key]) cityDeals[key] = [];
      cityDeals[key].push(deal);
    }

    // Geocode cities (batch with small delay to respect Nominatim rate limits)
    const cityKeys = Object.keys(cityDeals);
    const geocoded = {};
    const BATCH = 5;

    // Geocode using Photon (Komoot) — free, CORS-friendly
    let geocodeCount = 0;
    for (const cityKey of cityKeys) {
      if (geoCache[cityKey]) {
        geocoded[cityKey] = geoCache[cityKey];
        continue;
      }
      try {
        const resp = await fetch(`https://photon.komoot.io/api/?q=${encodeURIComponent(cityKey + ', USA')}&limit=1`);
        if (resp.ok) {
          const data = await resp.json();
          const feature = data.features?.[0];
          if (feature?.geometry?.coordinates) {
            const [lng, lat] = feature.geometry.coordinates;
            const loc = { lat, lng };
            geoCache[cityKey] = loc;
            geocoded[cityKey] = loc;
          }
        }
      } catch (e) {
        console.warn('Geocode failed for', cityKey);
      }
      geocodeCount++;
      // Small delay to be respectful
      if (geocodeCount % 5 === 0) {
        await new Promise(r => setTimeout(r, 150));
      }
      // Render markers progressively every 15 cities
      if (geocodeCount % 15 === 0) {
        renderMapMarkers(geocoded, cityDeals);
        saveGeoCache();
      }
    }

    // Persist geocache to localStorage
    saveGeoCache();
    console.log(`Map: ${Object.keys(geocoded).length} cities geocoded, ${cityKeys.length} total cities`);

    // Final render
    renderMapMarkers(geocoded, cityDeals);
  } catch (err) {
    console.error('Failed to load map data:', err);
  }
}

function renderMapMarkers(geocoded, cityDeals) {
  if (!dashMap) return;

  // Clear existing markers
  dashMap.eachLayer(layer => {
    if (layer instanceof L.CircleMarker) dashMap.removeLayer(layer);
  });

  // US bounding box (continental + Hawaii/Alaska)
  const isInUS = (lat, lng) => lat >= 18 && lat <= 72 && lng >= -180 && lng <= -65;

  const bounds = [];
  for (const [cityKey, loc] of Object.entries(geocoded)) {
    // Skip non-US locations
    if (!isInUS(loc.lat, loc.lng)) continue;

    const deals = cityDeals[cityKey] || [];
    for (const deal of deals) {
      const jLat = loc.lat + (Math.random() - 0.5) * 0.02;
      const jLng = loc.lng + (Math.random() - 0.5) * 0.02;

      const isMulti = deal.match_status === 'multi_match';
      const color = isMulti ? '#eab308' : '#22c55e';
      const fullAddr = deal.matched_address || 'Unknown';
      const askPrice = deal.parsed_asking_price ? '$' + Number(deal.parsed_asking_price).toLocaleString() : '';
      const arv = deal.parsed_arv ? '$' + Number(deal.parsed_arv).toLocaleString() : '';
      const beds = deal.parsed_beds || '?';
      const baths = deal.parsed_baths || '?';
      const sqft = deal.parsed_sqft ? Number(deal.parsed_sqft).toLocaleString() + 'sf' : '';
      const conf = deal.match_confidence ? `<span style="color:${deal.match_confidence === 'high' || deal.match_confidence === 'exact' ? '#22c55e' : deal.match_confidence === 'medium' ? '#eab308' : '#ef4444'}">${deal.match_confidence}</span>` : '';
      const streetViewUrl = `https://www.google.com/maps/@?api=1&map_action=pano&viewpoint=${jLat},${jLng}&heading=0&pitch=0`;
      const mapsUrl = `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(fullAddr)}`;

      const marker = L.circleMarker([jLat, jLng], {
        radius: 6,
        fillColor: color,
        color: 'rgba(0,0,0,0.3)',
        weight: 1,
        fillOpacity: 0.85,
      }).addTo(dashMap);

      marker.bindPopup(`
        <div style="min-width:220px">
          <div style="font-weight:700;color:${color};margin-bottom:6px;font-size:13px;">${escapeHtml(fullAddr)}</div>
          <div style="font-size:12px;color:#8b8fa3;line-height:1.7;">
            ${beds}bd / ${baths}ba ${sqft ? '· ' + sqft : ''}<br>
            ${askPrice ? '<span style="color:#e8eaf0;">Ask: </span><span style="font-weight:700;color:#e8eaf0;">' + askPrice + '</span><br>' : ''}
            ${arv ? '<span style="color:#e8eaf0;">ARV: </span><span style="font-weight:700;color:#22c55e;">' + arv + '</span><br>' : ''}
            ${isMulti ? '<span style="color:#eab308">Multi-Match</span>' : '<span style="color:#22c55e">Matched</span>'}
            ${conf ? ' · ' + conf : ''}
          </div>
          <div style="display:flex;gap:8px;margin-top:8px;border-top:1px solid rgba(255,255,255,0.08);padding-top:8px;">
            <a href="${streetViewUrl}" target="_blank" style="color:#818cf8;font-size:11px;font-weight:600;text-decoration:none;display:flex;align-items:center;gap:3px;">
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 2a14.5 14.5 0 0 0 0 20 14.5 14.5 0 0 0 0-20"/><path d="M2 12h20"/></svg>
              Street View
            </a>
            <a href="${mapsUrl}" target="_blank" style="color:#818cf8;font-size:11px;font-weight:600;text-decoration:none;display:flex;align-items:center;gap:3px;">
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"/><circle cx="12" cy="10" r="3"/></svg>
              Google Maps
            </a>
          </div>
        </div>
      `);

      bounds.push([jLat, jLng]);
    }
  }

  if (bounds.length > 0) {
    dashMap.fitBounds(bounds, { padding: [30, 30], maxZoom: 12 });
  }
}

async function loadDeals() {
  const offset = currentPage * PAGE_SIZE;
  const filters = statusFilters(currentFilter);

  try {
    const { data, count } = await supabaseGet('fb_deal_posts', {
      filters,
      order: 'captured_at.desc',
      limit: PAGE_SIZE,
      offset,
    });

    allDeals = cleanDeals(data);
    totalCount = count;
    renderDealsToTable(allDeals, 'dealsBody');
    renderPaginationTo('pagination', count, currentPage);

    document.getElementById('loadingState').style.display = 'none';
    document.getElementById('dealsTable').style.display = '';
    document.getElementById('dealCount').textContent = `${count} deal${count !== 1 ? 's' : ''}`;
    document.getElementById('navDealCount').textContent = count.toLocaleString();
  } catch (err) {
    console.error('Failed to load deals:', err);
    document.getElementById('loadingState').innerHTML =
      `<div style="color:var(--red)">Failed to load deals: ${escapeHtml(err.message)}</div>`;
  }
}

async function loadKPIs() {
  try {
    const statuses = ['matched', 'multi_match', 'confirmed', 'no_match', 'pending'];
    const counts = {};

    const { count: total } = await supabaseGet('fb_deal_posts', {
      select: 'id',
      limit: 0,
    });
    counts.total = total;

    // Get status counts + GMV data in parallel
    const gmvPromise = supabaseGet('fb_deal_posts', {
      select: 'parsed_arv,parsed_asking_price,matched_address',
      filters: [{ col: 'match_status', val: 'in.(matched,multi_match,confirmed)' }],
      limit: 5000,
    });

    await Promise.all(statuses.map(async (s) => {
      const { count } = await supabaseGet('fb_deal_posts', {
        select: 'id',
        filters: [{ col: 'match_status', val: `eq.${s}` }],
        limit: 0,
      });
      counts[s] = count;
    }));

    // Calculate GMV (prefer ARV, fallback to asking price) - dedup by matched address
    const { data: gmvRaw } = await gmvPromise;
    const gmvData = deduplicateDeals(gmvRaw);
    let gmv = 0;
    for (const d of gmvData) {
      const val = d.parsed_arv || d.parsed_asking_price;
      if (val) gmv += Number(val);
    }
    const gmvDisplay = gmv >= 1000000 ? '$' + (gmv / 1000000).toFixed(1) + 'M' : gmv >= 1000 ? '$' + (gmv / 1000).toFixed(0) + 'K' : '$' + gmv;

    const processed = (counts.total || 0) - (counts.pending || 0);

    document.getElementById('kpiGrid').innerHTML = `
      <div class="kpi-card blue">
        <div class="kpi-glow"></div>
        <div class="kpi-value">${(counts.total || 0).toLocaleString()}</div>
        <div class="kpi-label">Total Leads</div>
      </div>
      <div class="kpi-card purple">
        <div class="kpi-glow"></div>
        <div class="kpi-value">${processed.toLocaleString()}</div>
        <div class="kpi-label">Processed</div>
      </div>
      <div class="kpi-card green">
        <div class="kpi-glow"></div>
        <div class="kpi-value">${(counts.matched || 0).toLocaleString()}</div>
        <div class="kpi-label">Matched</div>
      </div>
      <div class="kpi-card yellow">
        <div class="kpi-glow"></div>
        <div class="kpi-value">${(counts.multi_match || 0).toLocaleString()}</div>
        <div class="kpi-label">Multi-Match</div>
      </div>
      <div class="kpi-card red">
        <div class="kpi-glow"></div>
        <div class="kpi-value">${(counts.no_match || 0).toLocaleString()}</div>
        <div class="kpi-label">No Match</div>
      </div>
      <div class="kpi-card cyan">
        <div class="kpi-glow"></div>
        <div class="kpi-value">${gmvDisplay}</div>
        <div class="kpi-label">Pipeline GMV</div>
      </div>
    `;
  } catch (err) {
    console.error('Failed to load KPIs:', err);
  }
}

// ===== WHOLESALERS =====
let wholesalersData = [];
let wholesalerSortCol = 'matched';
let wholesalerSortDir = 'desc';
let selectedWholesaler = null;
let wholesalerDeals = [];
let wholesalerDealPage = 0;

async function loadWholesalers() {
  const container = document.getElementById('wholesalersContent');
  if (!container) return;

  // Show loading if no data yet
  if (wholesalersData.length === 0) {
    container.innerHTML = `<div class="loading"><div class="spinner"></div><div>Analyzing wholesalers...</div></div>`;
  }

  try {
    // Fetch all processed deals with poster info
    const allPosts = [];
    let offset = 0;
    const batchSize = 1000;
    let keepGoing = true;

    while (keepGoing) {
      const { data, count } = await supabaseGet('fb_deal_posts', {
        select: 'poster_name,group_name,match_status,captured_at,parsed_asking_price,matched_address',
        filters: [{ col: 'match_status', val: 'neq.pending' }],
        order: 'captured_at.desc',
        limit: batchSize,
        offset,
      });
      allPosts.push(...data);
      offset += batchSize;
      if (data.length < batchSize || allPosts.length >= count) keepGoing = false;
    }

    // Deduplicate deals across all wholesalers
    const dedupedPosts = deduplicateDeals(allPosts);

    // Aggregate by poster_name (clean up scraper artifacts like "· Follow")
    const map = {};
    for (const post of dedupedPosts) {
      const name = cleanPosterName(post.poster_name || 'Unknown');
      const rawName = post.poster_name || 'Unknown';
      if (!map[name]) {
        map[name] = { name, rawNames: new Set(), total: 0, matched: 0, multi: 0, noMatch: 0, confirmed: 0, groups: new Set(), lastActive: null, prices: [] };
      }
      map[name].rawNames.add(rawName);
      const w = map[name];
      w.total++;
      if (post.match_status === 'matched') w.matched++;
      else if (post.match_status === 'multi_match') w.multi++;
      else if (post.match_status === 'no_match') w.noMatch++;
      else if (post.match_status === 'confirmed') w.confirmed++;
      if (post.group_name) w.groups.add(post.group_name);
      if (post.parsed_asking_price) w.prices.push(Number(post.parsed_asking_price));
      const d = new Date(post.captured_at);
      if (!w.lastActive || d > w.lastActive) w.lastActive = d;
    }

    wholesalersData = Object.values(map).map(w => ({
      ...w,
      rawNames: Array.from(w.rawNames),
      groups: Array.from(w.groups),
      matchRate: w.total > 0 ? Math.round(((w.matched + w.multi + w.confirmed) / w.total) * 100) : 0,
      avgPrice: w.prices.length > 0 ? Math.round(w.prices.reduce((a, b) => a + b, 0) / w.prices.length) : 0,
    }));

    renderWholesalers();
  } catch (err) {
    console.error('Failed to load wholesalers:', err);
    container.innerHTML = `<div style="color:var(--red);text-align:center;padding:40px;">Failed to load wholesalers: ${escapeHtml(err.message)}</div>`;
  }
}

function renderWholesalers() {
  const container = document.getElementById('wholesalersContent');
  if (!container) return;

  // Sort
  const sorted = [...wholesalersData].sort((a, b) => {
    let av = a[wholesalerSortCol], bv = b[wholesalerSortCol];
    if (wholesalerSortCol === 'name') {
      av = (av || '').toLowerCase();
      bv = (bv || '').toLowerCase();
      return wholesalerSortDir === 'asc' ? av.localeCompare(bv) : bv.localeCompare(av);
    }
    if (wholesalerSortCol === 'lastActive') {
      av = av ? av.getTime() : 0;
      bv = bv ? bv.getTime() : 0;
    }
    return wholesalerSortDir === 'asc' ? av - bv : bv - av;
  });

  const sortIcon = (col) => {
    if (wholesalerSortCol !== col) return '';
    return wholesalerSortDir === 'asc' ? ' &#9650;' : ' &#9660;';
  };

  const kpiMatched = wholesalersData.reduce((s, w) => s + w.matched + w.multi + w.confirmed, 0);
  const kpiTotal = wholesalersData.reduce((s, w) => s + w.total, 0);
  const avgMatchRate = kpiTotal > 0 ? Math.round((kpiMatched / kpiTotal) * 100) : 0;

  container.innerHTML = `
    <div class="kpi-grid" style="grid-template-columns:repeat(4,1fr);margin-bottom:20px;">
      <div class="kpi-card blue">
        <div class="kpi-glow"></div>
        <div class="kpi-value">${wholesalersData.length.toLocaleString()}</div>
        <div class="kpi-label">Wholesalers</div>
      </div>
      <div class="kpi-card green">
        <div class="kpi-glow"></div>
        <div class="kpi-value">${kpiTotal.toLocaleString()}</div>
        <div class="kpi-label">Total Posts</div>
      </div>
      <div class="kpi-card purple">
        <div class="kpi-glow"></div>
        <div class="kpi-value">${avgMatchRate}%</div>
        <div class="kpi-label">Avg Match Rate</div>
      </div>
      <div class="kpi-card cyan">
        <div class="kpi-glow"></div>
        <div class="kpi-value">${new Set(wholesalersData.flatMap(w => w.groups)).size}</div>
        <div class="kpi-label">Active Groups</div>
      </div>
    </div>
    <div class="toolbar">
      <div class="toolbar-left">
        <input type="text" id="wholesalerSearch" placeholder="Search wholesalers..." oninput="filterWholesalerTable()" style="padding:8px 14px;border:1px solid var(--border);border-radius:8px;font-size:13px;font-family:inherit;background:var(--surface);color:var(--text);outline:none;width:260px;">
      </div>
      <div class="toolbar-right">
        <span class="deal-count-label">${sorted.length} wholesalers</span>
      </div>
    </div>
    <table class="deals-table" id="wholesalerTable">
      <thead>
        <tr>
          <th style="cursor:pointer" onclick="sortWholesalers('name')">Wholesaler${sortIcon('name')}</th>
          <th style="cursor:pointer" onclick="sortWholesalers('total')">Posts${sortIcon('total')}</th>
          <th style="cursor:pointer" onclick="sortWholesalers('matched')">Matched${sortIcon('matched')}</th>
          <th style="cursor:pointer" onclick="sortWholesalers('multi')">Multi${sortIcon('multi')}</th>
          <th style="cursor:pointer" onclick="sortWholesalers('noMatch')">No Match${sortIcon('noMatch')}</th>
          <th style="cursor:pointer" onclick="sortWholesalers('matchRate')">Match Rate${sortIcon('matchRate')}</th>
          <th style="cursor:pointer" onclick="sortWholesalers('avgPrice')">Avg Price${sortIcon('avgPrice')}</th>
          <th>Groups</th>
          <th style="cursor:pointer" onclick="sortWholesalers('lastActive')">Last Active${sortIcon('lastActive')}</th>
        </tr>
      </thead>
      <tbody id="wholesalerBody">
        ${sorted.map(w => {
          const rateColor = w.matchRate >= 60 ? 'var(--green)' : w.matchRate >= 30 ? 'var(--yellow)' : 'var(--red)';
          const rateBg = w.matchRate >= 60 ? 'var(--green-bg)' : w.matchRate >= 30 ? 'var(--yellow-bg)' : 'var(--red-bg)';
          return `
          <tr class="wholesaler-row" data-wholesaler="${encodeURIComponent(w.name)}" onclick="openWholesalerDetail(decodeURIComponent(this.dataset.wholesaler))" style="cursor:pointer">
            <td style="font-weight:600;color:var(--accent-hover)">
              ${escapeHtml(w.name)}
              ${(() => {
                const ck = `omr_contact_${safeBtoa(w.name)}`;
                const sv = JSON.parse(localStorage.getItem(ck) || '{}');
                const fbHref = sv.facebook || `https://www.facebook.com/search/people/?q=${encodeURIComponent(w.name)}`;
                const hasFb = !!sv.facebook;
                return `<a href="${fbHref}" target="_blank" onclick="event.stopPropagation();" style="margin-left:6px;color:${hasFb ? 'var(--accent)' : 'var(--text-light)'};opacity:${hasFb ? '1' : '0.5'};transition:opacity 0.15s;" onmouseover="this.style.opacity=1" onmouseout="this.style.opacity=${hasFb ? '1' : '0.5'}" title="${hasFb ? 'View Facebook Profile' : 'Search on Facebook'}">
                  <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px;"><path d="M18 2h-3a5 5 0 0 0-5 5v3H7v4h3v8h4v-8h3l1-4h-4V7a1 1 0 0 1 1-1h3z"/></svg>
                </a>`;
              })()}
            </td>
            <td style="font-weight:700">${w.total}</td>
            <td style="color:var(--green)">${w.matched}</td>
            <td style="color:var(--yellow)">${w.multi}</td>
            <td style="color:var(--text-light)">${w.noMatch}</td>
            <td>
              <div style="display:flex;align-items:center;gap:8px;">
                <div style="flex:1;height:6px;background:rgba(255,255,255,0.05);border-radius:3px;overflow:hidden;min-width:50px;">
                  <div style="height:100%;width:${w.matchRate}%;background:${rateColor};border-radius:3px;"></div>
                </div>
                <span style="font-weight:600;font-size:12px;color:${rateColor}">${w.matchRate}%</span>
              </div>
            </td>
            <td>${w.avgPrice ? '$' + w.avgPrice.toLocaleString() : '-'}</td>
            <td style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--text-muted);font-size:12px">${w.groups.join(', ')}</td>
            <td style="color:var(--text-muted);font-size:12px">${w.lastActive ? formatDate(w.lastActive.toISOString()) : '-'}</td>
          </tr>`;
        }).join('')}
      </tbody>
    </table>
  `;
}

function toggleContactForm() {
  const wrapper = document.getElementById('wsContactFormWrapper');
  if (!wrapper) return;
  if (wrapper.style.maxHeight === '0px' || wrapper.style.maxHeight === '0') {
    wrapper.style.maxHeight = '200px';
    wrapper.style.marginBottom = '16px';
  } else {
    wrapper.style.maxHeight = '0';
    wrapper.style.marginBottom = '0';
  }
}

function buildContactLinks(data) {
  const iconBtn = (href, title, bg, color, svgPath) =>
    `<a href="${href}" target="_blank" title="${escapeHtml(title)}" style="display:inline-flex;align-items:center;justify-content:center;width:28px;height:28px;border-radius:6px;background:${bg};color:${color};transition:opacity 0.15s;text-decoration:none;" onmouseover="this.style.opacity=0.8" onmouseout="this.style.opacity=1">${svgPath}</a>`;
  const svgFb = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 2h-3a5 5 0 0 0-5 5v3H7v4h3v8h4v-8h3l1-4h-4V7a1 1 0 0 1 1-1h3z"/></svg>';
  const svgDm = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>';
  const svgEmail = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><polyline points="22,6 12,13 2,6"/></svg>';
  const svgPhone = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07 19.5 19.5 0 0 1-6-6 19.79 19.79 0 0 1-3.07-8.67A2 2 0 0 1 4.11 2h3a2 2 0 0 1 2 1.72c.127.96.361 1.903.7 2.81a2 2 0 0 1-.45 2.11L8.09 9.91a16 16 0 0 0 6 6l1.27-1.27a2 2 0 0 1 2.11-.45c.907.339 1.85.573 2.81.7A2 2 0 0 1 22 16.92z"/></svg>';
  let html = '';
  if (data.facebook) {
    html += iconBtn(data.facebook, 'Facebook Profile', 'var(--accent)', '#fff', svgFb);
    html += iconBtn(data.facebook.replace(/\/$/, '') + '/messages', 'Send DM', 'var(--purple)', '#fff', svgDm);
  }
  if (data.email) html += iconBtn('mailto:' + data.email, data.email, 'var(--green)', '#fff', svgEmail);
  if (data.phone) html += iconBtn('tel:' + data.phone.replace(/[^+\d]/g, ''), data.phone, 'var(--yellow)', '#1a1a2e', svgPhone);
  if (data.company) html += `<span style="font-size:12px;color:var(--text-muted);margin-left:4px;">${escapeHtml(data.company)}</span>`;
  return html;
}

function saveWholesalerContact(name) {
  const contactKey = `omr_contact_${safeBtoa(name)}`;
  const data = {
    facebook: document.getElementById('ws_facebook')?.value?.trim() || '',
    email: document.getElementById('ws_email')?.value?.trim() || '',
    phone: document.getElementById('ws_phone')?.value?.trim() || '',
    company: document.getElementById('ws_company')?.value?.trim() || '',
    notes: document.getElementById('ws_notes')?.value?.trim() || '',
  };
  localStorage.setItem(contactKey, JSON.stringify(data));

  // Rebuild contact links next to name
  const linksEl = document.getElementById('wsContactLinks');
  if (linksEl) linksEl.innerHTML = buildContactLinks(data);

  const status = document.getElementById('wsSaveStatus');
  if (status) {
    status.style.display = 'block';
    setTimeout(() => { status.style.display = 'none'; }, 2000);
  }
}

function sortWholesalers(col) {
  if (wholesalerSortCol === col) {
    wholesalerSortDir = wholesalerSortDir === 'asc' ? 'desc' : 'asc';
  } else {
    wholesalerSortCol = col;
    wholesalerSortDir = col === 'name' ? 'asc' : 'desc';
  }
  renderWholesalers();
}

function filterWholesalerTable() {
  const q = (document.getElementById('wholesalerSearch')?.value || '').toLowerCase();
  const rows = document.querySelectorAll('#wholesalerBody .wholesaler-row');
  rows.forEach(row => {
    const name = row.children[0].textContent.toLowerCase();
    row.style.display = name.includes(q) ? '' : 'none';
  });
}

// Normalize phone number to (xxx) xxx-xxxx format
function normalizePhone(raw) {
  const digits = raw.replace(/\D/g, '');
  // Handle 11-digit with leading 1
  const d = digits.length === 11 && digits[0] === '1' ? digits.slice(1) : digits;
  if (d.length !== 10) return raw; // can't normalize non-10-digit
  return `(${d.slice(0,3)}) ${d.slice(3,6)}-${d.slice(6)}`;
}

// Extract phone numbers and emails from post texts
function extractContactInfo(posts) {
  const phones = new Set();
  const emails = new Set();
  for (const p of posts) {
    if (!p.post_text) continue;
    // Phone patterns: (xxx) xxx-xxxx, xxx-xxx-xxxx, xxx.xxx.xxxx, xxxxxxxxxx
    const phoneMatches = p.post_text.match(/(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})/g);
    if (phoneMatches) phoneMatches.forEach(ph => phones.add(normalizePhone(ph.trim())));
    // Email patterns
    const emailMatches = p.post_text.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g);
    if (emailMatches) emailMatches.forEach(em => emails.add(em.toLowerCase().trim()));
  }
  return { phones: [...phones], emails: [...emails] };
}

let wholesalerDetailMap = null;

async function openWholesalerDetail(name) {
  selectedWholesaler = name;
  wholesalerDealPage = 0;

  const container = document.getElementById('wholesalersContent');
  container.innerHTML = `<div class="loading"><div class="spinner"></div><div>Loading deals for ${escapeHtml(name)}...</div></div>`;

  try {
    // Look up raw names (with scraper artifacts) to query DB correctly
    const ws0 = wholesalersData.find(w => w.name === name);
    const rawNames = ws0?.rawNames || [name];
    const posterFilter = rawNames.length === 1
      ? { col: 'poster_name', val: `eq.${rawNames[0]}` }
      : { col: 'poster_name', val: `in.(${rawNames.map(n => `"${n}"`).join(',')})` };

    // Fetch ALL posts (with post_text for contact extraction) - up to 2000
    const allPosts = [];
    let fetchOffset = 0;
    const batchSize = 1000;
    let keepGoing = true;
    while (keepGoing) {
      const { data, count } = await supabaseGet('fb_deal_posts', {
        select: 'id,post_text,post_url,poster_name,group_name,match_status,captured_at,parsed_asking_price,parsed_arv,parsed_beds,parsed_baths,parsed_sqft,parsed_city,parsed_state,parsed_zip,parsed_full_address,matched_address,match_count,match_confidence,match_candidates',
        filters: [
          posterFilter,
          { col: 'match_status', val: 'neq.pending' },
        ],
        order: 'captured_at.desc',
        limit: batchSize,
        offset: fetchOffset,
      });
      allPosts.push(...data);
      fetchOffset += batchSize;
      if (data.length < batchSize || allPosts.length >= (count || 0)) keepGoing = false;
    }

    const cleanedPosts = cleanDeals(allPosts);
    const totalCount = cleanedPosts.length;
    wholesalerDeals = cleanedPosts;

    // Extract contact info from post texts (use raw allPosts to find all contacts)
    const extracted = extractContactInfo(allPosts);

    // Find wholesaler stats
    const ws = wholesalersData.find(w => w.name === name) || {};

    // Load saved contact info from localStorage
    const contactKey = `omr_contact_${safeBtoa(name)}`;
    const saved = JSON.parse(localStorage.getItem(contactKey) || '{}');
    const fbSearch = `https://www.facebook.com/search/people/?q=${encodeURIComponent(name)}`;
    const fbUrl = saved.facebook || '';
    // Auto-fill from extracted if saved fields are empty
    const autoPhone = saved.phone || extracted.phones[0] || '';
    const autoEmail = saved.email || extracted.emails[0] || '';
    const inputStyle = 'width:100%;padding:7px 10px;border:1px solid var(--border);border-radius:6px;font-size:12px;font-family:inherit;background:var(--surface);color:var(--text);outline:none;';

    // Filter matched deals for map and table
    const matchedDeals = cleanedPosts.filter(d => d.match_status === 'matched' || d.match_status === 'multi_match');

    // Calculate GMV from matched deals
    let gmv = 0;
    for (const d of matchedDeals) {
      const val = Number(d.parsed_arv) || Number(d.parsed_asking_price) || 0;
      gmv += val;
    }

    container.innerHTML = `
      <button class="btn" onclick="renderWholesalers()" style="margin-bottom:16px;">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="15 18 9 12 15 6"/></svg>
        Back to Directory
      </button>
      <div style="display:flex;align-items:center;gap:16px;margin-bottom:16px;">
        <div style="width:48px;height:48px;border-radius:12px;background:linear-gradient(135deg,var(--accent),var(--purple));display:flex;align-items:center;justify-content:center;font-size:20px;font-weight:700;color:#fff;flex-shrink:0;">${(name[0] || '?').toUpperCase()}</div>
        <div style="flex:1;min-width:0;">
          <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">
            <span style="font-size:20px;font-weight:700;">${escapeHtml(name)}</span>
            <div id="wsContactLinks" style="display:flex;align-items:center;gap:6px;">
              ${fbUrl ? `<a href="${fbUrl}" target="_blank" title="Facebook Profile" style="display:inline-flex;align-items:center;justify-content:center;width:28px;height:28px;border-radius:6px;background:var(--accent);color:#fff;transition:opacity 0.15s;" onmouseover="this.style.opacity=0.8" onmouseout="this.style.opacity=1">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 2h-3a5 5 0 0 0-5 5v3H7v4h3v8h4v-8h3l1-4h-4V7a1 1 0 0 1 1-1h3z"/></svg>
              </a>` : ''}
              ${fbUrl ? `<a href="${fbUrl.replace(/\/$/, '')}/messages" target="_blank" title="Send DM" style="display:inline-flex;align-items:center;justify-content:center;width:28px;height:28px;border-radius:6px;background:var(--purple);color:#fff;transition:opacity 0.15s;" onmouseover="this.style.opacity=0.8" onmouseout="this.style.opacity=1">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>
              </a>` : ''}
              ${autoEmail ? `<a href="mailto:${escapeHtml(autoEmail)}" title="${escapeHtml(autoEmail)}" style="display:inline-flex;align-items:center;justify-content:center;width:28px;height:28px;border-radius:6px;background:var(--green);color:#fff;transition:opacity 0.15s;" onmouseover="this.style.opacity=0.8" onmouseout="this.style.opacity=1">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><polyline points="22,6 12,13 2,6"/></svg>
              </a>` : ''}
              ${autoPhone ? `<a href="tel:${autoPhone.replace(/[^+\d]/g, '')}" title="${escapeHtml(autoPhone)}" style="display:inline-flex;align-items:center;justify-content:center;width:28px;height:28px;border-radius:6px;background:var(--yellow);color:#1a1a2e;transition:opacity 0.15s;" onmouseover="this.style.opacity=0.8" onmouseout="this.style.opacity=1">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07 19.5 19.5 0 0 1-6-6 19.79 19.79 0 0 1-3.07-8.67A2 2 0 0 1 4.11 2h3a2 2 0 0 1 2 1.72c.127.96.361 1.903.7 2.81a2 2 0 0 1-.45 2.11L8.09 9.91a16 16 0 0 0 6 6l1.27-1.27a2 2 0 0 1 2.11-.45c.907.339 1.85.573 2.81.7A2 2 0 0 1 22 16.92z"/></svg>
              </a>` : ''}
              ${saved.company ? `<span style="font-size:12px;color:var(--text-muted);margin-left:4px;">${escapeHtml(saved.company)}</span>` : ''}
            </div>
            <button onclick="toggleContactForm()" class="btn" style="padding:4px 8px;font-size:11px;gap:4px;opacity:0.6;" title="Edit contact info" onmouseover="this.style.opacity=1" onmouseout="this.style.opacity=0.6">
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>
              Edit
            </button>
          </div>
          <div style="font-size:13px;color:var(--text-muted);margin-top:2px;">${totalCount} total posts &middot; ${ws.matchRate || 0}% match rate &middot; Groups: ${(ws.groups || []).join(', ')}</div>
        </div>
      </div>
      <div id="wsContactFormWrapper" style="max-height:0;overflow:hidden;transition:max-height 0.3s ease;margin-bottom:0;">
        <div style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:14px 18px;margin-bottom:16px;" id="wholesalerContactForm">
          <div style="display:flex;gap:12px;align-items:flex-end;flex-wrap:wrap;">
            <div style="flex:1;min-width:160px;">
              <label style="font-size:10px;font-weight:600;color:var(--text-light);text-transform:uppercase;letter-spacing:0.5px;display:block;margin-bottom:3px;">Facebook URL</label>
              <input type="text" id="ws_facebook" value="${escapeHtml(fbUrl)}" placeholder="https://facebook.com/..." style="${inputStyle}">
            </div>
            <div style="flex:1;min-width:150px;">
              <label style="font-size:10px;font-weight:600;color:var(--text-light);text-transform:uppercase;letter-spacing:0.5px;display:block;margin-bottom:3px;">Email${!saved.email && autoEmail ? ' <span style="color:var(--accent);font-weight:400;text-transform:none;">(auto-detected)</span>' : ''}</label>
              <input type="email" id="ws_email" value="${escapeHtml(autoEmail)}" placeholder="email@example.com" style="${inputStyle}${!saved.email && autoEmail ? 'border-color:var(--accent);' : ''}">
            </div>
            <div style="flex:0 0 150px;">
              <label style="font-size:10px;font-weight:600;color:var(--text-light);text-transform:uppercase;letter-spacing:0.5px;display:block;margin-bottom:3px;">Phone${!saved.phone && autoPhone ? ' <span style="color:var(--accent);font-weight:400;text-transform:none;">(auto-detected)</span>' : ''}</label>
              <input type="tel" id="ws_phone" value="${escapeHtml(autoPhone)}" placeholder="(555) 123-4567" style="${inputStyle}${!saved.phone && autoPhone ? 'border-color:var(--accent);' : ''}">
            </div>
            <div style="flex:1;min-width:140px;">
              <label style="font-size:10px;font-weight:600;color:var(--text-light);text-transform:uppercase;letter-spacing:0.5px;display:block;margin-bottom:3px;">Company</label>
              <input type="text" id="ws_company" value="${escapeHtml(saved.company || '')}" placeholder="Company name" style="${inputStyle}">
            </div>
            <div style="flex:2;min-width:180px;">
              <label style="font-size:10px;font-weight:600;color:var(--text-light);text-transform:uppercase;letter-spacing:0.5px;display:block;margin-bottom:3px;">Notes</label>
              <input type="text" id="ws_notes" value="${escapeHtml(saved.notes || '')}" placeholder="Quick notes..." style="${inputStyle}">
            </div>
            <button class="btn btn-primary" onclick="saveWholesalerContact('${escapeHtml(name)}')" style="padding:7px 14px;font-size:12px;flex-shrink:0;">
              Save
            </button>
            <div id="wsSaveStatus" style="font-size:11px;color:var(--green);display:none;flex-shrink:0;">Saved!</div>
          </div>
          ${extracted.phones.length > 1 || extracted.emails.length > 1 ? `
          <div style="margin-top:8px;font-size:11px;color:var(--text-light);">
            ${extracted.phones.length > 1 ? `<span>📞 All phones found: ${extracted.phones.join(', ')}</span>` : ''}
            ${extracted.emails.length > 1 ? `<span style="margin-left:12px;">📧 All emails found: ${extracted.emails.join(', ')}</span>` : ''}
          </div>` : ''}
        </div>
      </div>
      <div class="kpi-grid" style="grid-template-columns:repeat(6,1fr);margin-bottom:20px;">
        <div class="kpi-card blue">
          <div class="kpi-glow"></div>
          <div class="kpi-value">${totalCount}</div>
          <div class="kpi-label">Total Posts</div>
        </div>
        <div class="kpi-card green">
          <div class="kpi-glow"></div>
          <div class="kpi-value">${ws.matched || 0}</div>
          <div class="kpi-label">Matched</div>
        </div>
        <div class="kpi-card yellow">
          <div class="kpi-glow"></div>
          <div class="kpi-value">${ws.multi || 0}</div>
          <div class="kpi-label">Multi-Match</div>
        </div>
        <div class="kpi-card red">
          <div class="kpi-glow"></div>
          <div class="kpi-value">${ws.noMatch || 0}</div>
          <div class="kpi-label">No Match</div>
        </div>
        <div class="kpi-card purple">
          <div class="kpi-glow"></div>
          <div class="kpi-value">${ws.avgPrice ? '$' + (ws.avgPrice / 1000).toFixed(0) + 'k' : '-'}</div>
          <div class="kpi-label">Avg Price</div>
        </div>
        <div class="kpi-card green">
          <div class="kpi-glow"></div>
          <div class="kpi-value">${gmv > 0 ? '$' + (gmv >= 1000000 ? (gmv / 1000000).toFixed(1) + 'M' : (gmv / 1000).toFixed(0) + 'k') : '-'}</div>
          <div class="kpi-label">Total GMV</div>
        </div>
      </div>

      <!-- Map + Table side by side -->
      <div style="display:flex;gap:16px;margin-bottom:20px;">
        <div style="flex:1;min-width:0;">
          <div style="font-size:13px;font-weight:600;margin-bottom:8px;color:var(--text-light);">Deal Map (${matchedDeals.length} matched)</div>
          <div id="wholesalerMap" style="height:360px;border-radius:var(--radius);border:1px solid var(--border);overflow:hidden;"></div>
        </div>
        <div style="flex:1;min-width:0;max-height:400px;overflow-y:auto;">
          <div style="font-size:13px;font-weight:600;margin-bottom:8px;color:var(--text-light);">Matched Deals</div>
          <table class="deals-table" style="font-size:12px;">
            <thead>
              <tr>
                <th>Date</th>
                <th>Address</th>
                <th>Bd/Ba/SqFt</th>
                <th>Ask</th>
                <th>Post</th>
              </tr>
            </thead>
            <tbody id="wholesalerMatchedBody"></tbody>
          </table>
        </div>
      </div>

      <details style="margin-bottom:16px;">
        <summary style="cursor:pointer;font-size:13px;font-weight:600;color:var(--text-light);padding:8px 0;">All Posts (${totalCount})</summary>
        <table class="deals-table" style="margin-top:8px;">
          <thead>
            <tr>
              <th>Date</th>
              <th>Group</th>
              <th>Parsed Address</th>
              <th>Resolved Address</th>
              <th>Beds/Bath/SqFt</th>
              <th>Price</th>
              <th>Status</th>
              <th>#</th>
              <th>Confidence</th>
            </tr>
          </thead>
          <tbody id="wholesalerDealsBody"></tbody>
        </table>
      </details>
    `;

    // Render matched deals table (simplified)
    const matchedBody = document.getElementById('wholesalerMatchedBody');
    for (const deal of matchedDeals) {
      const addr = buildFullAddress(deal) || parsedAddress(deal) || '-';
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${formatDate(deal.captured_at)}</td>
        <td style="font-weight:600;">${escapeHtml(addr)}</td>
        <td>${deal.parsed_beds || '?'}/${deal.parsed_baths || '?'} &middot; ${deal.parsed_sqft ? Number(deal.parsed_sqft).toLocaleString() + 'sf' : '?'}</td>
        <td>${formatPrice(deal.parsed_asking_price)}</td>
        <td>${deal.post_url ? `<a href="${deal.post_url}" target="_blank" onclick="event.stopPropagation();" style="color:var(--accent);">View</a>` : '-'}</td>
      `;
      tr.addEventListener('click', () => toggleDetail(deal, tr));
      tr.style.cursor = 'pointer';
      matchedBody.appendChild(tr);
    }

    // Render all deals table
    const tbody = document.getElementById('wholesalerDealsBody');
    for (const deal of cleanedPosts) {
      const tr = document.createElement('tr');
      if (deal.match_status === 'no_match') tr.classList.add('row-no-match');
      const addrColor = addressColor(deal);
      tr.innerHTML = `
        <td>${formatDate(deal.captured_at)}</td>
        <td style="max-width:140px;overflow:hidden;text-overflow:ellipsis">${escapeHtml(deal.group_name || '-')}</td>
        <td>${escapeHtml(parsedAddress(deal))}</td>
        <td style="font-weight:600;color:${addrColor}">${escapeHtml(buildFullAddress(deal))}</td>
        <td>${deal.parsed_beds || '?'}/${deal.parsed_baths || '?'} &middot; ${deal.parsed_sqft ? Number(deal.parsed_sqft).toLocaleString() + 'sf' : '?'}</td>
        <td>${formatPrice(deal.parsed_asking_price)}</td>
        <td><span class="badge badge-${deal.match_status}">${(deal.match_status || '').replace('_', ' ')}</span></td>
        <td style="text-align:center;font-weight:600;color:${deal.match_count === 1 ? 'var(--green)' : deal.match_count > 1 ? 'var(--yellow)' : 'var(--text-light)'}">${deal.match_count || '-'}</td>
        <td>${deal.match_confidence ? `<span class="badge badge-${deal.match_confidence}">${deal.match_confidence}</span>` : '-'}</td>
      `;
      tr.addEventListener('click', () => toggleDetail(deal, tr));
      tbody.appendChild(tr);
    }

    // Render wholesaler map
    renderWholesalerMap(matchedDeals);

  } catch (err) {
    console.error('Failed to load wholesaler deals:', err);
    container.innerHTML = `
      <button class="btn" onclick="renderWholesalers()" style="margin-bottom:16px;">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="15 18 9 12 15 6"/></svg>
        Back to Directory
      </button>
      <div style="color:var(--red);text-align:center;padding:40px;">Failed to load deals: ${escapeHtml(err.message)}</div>
    `;
  }
}

async function renderWholesalerMap(deals) {
  const mapEl = document.getElementById('wholesalerMap');
  if (!mapEl) return;

  // Clean up previous map
  if (wholesalerDetailMap) {
    wholesalerDetailMap.remove();
    wholesalerDetailMap = null;
  }

  wholesalerDetailMap = L.map(mapEl).setView([39.8, -98.5], 4);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; OSM',
    maxZoom: 18,
  }).addTo(wholesalerDetailMap);

  const geocodeCache = JSON.parse(localStorage.getItem('omr_geocache') || '{}');
  const bounds = [];

  for (const deal of deals) {
    const c = deal.match_candidates?.[0];
    const city = c?.property_address_city || deal.parsed_city || '';
    const state = c?.property_address_state || deal.parsed_state || '';
    const addr = buildFullAddress(deal) || `${city}, ${state}`;
    if (!addr || addr.trim() === ',') continue;

    const cacheKey = addr.toUpperCase().trim();
    let lat, lng;

    if (geocodeCache[cacheKey]) {
      [lat, lng] = geocodeCache[cacheKey];
    } else {
      // Try geocoding
      try {
        const resp = await fetch(`https://photon.komoot.io/api/?q=${encodeURIComponent(addr)}&limit=1`);
        const json = await resp.json();
        const feat = json.features?.[0];
        if (feat) {
          lng = feat.geometry.coordinates[0];
          lat = feat.geometry.coordinates[1];
          if (lat >= 18 && lat <= 72 && lng >= -180 && lng <= -65) {
            geocodeCache[cacheKey] = [lat, lng];
            localStorage.setItem('omr_geocache', JSON.stringify(geocodeCache));
          } else {
            continue;
          }
        } else continue;
      } catch { continue; }
    }

    if (!lat || !lng) continue;
    if (lat < 18 || lat > 72 || lng < -180 || lng > -65) continue;

    bounds.push([lat, lng]);
    const color = deal.match_status === 'matched' ? '#4caf50' : '#ff9800';
    const marker = L.circleMarker([lat, lng], {
      radius: 7, fillColor: color, color: '#fff', weight: 1, fillOpacity: 0.85,
    }).addTo(wholesalerDetailMap);

    const price = deal.parsed_asking_price ? '$' + Number(deal.parsed_asking_price).toLocaleString() : '-';
    const arv = deal.parsed_arv ? '$' + Number(deal.parsed_arv).toLocaleString() : '-';
    const svLink = `https://www.google.com/maps/@?api=1&map_action=pano&viewpoint=${lat},${lng}`;
    marker.bindPopup(`
      <div style="font-size:12px;line-height:1.5;">
        <strong>${escapeHtml(addr)}</strong><br>
        Ask: ${price} &middot; ARV: ${arv}<br>
        ${deal.parsed_beds || '?'}bd/${deal.parsed_baths || '?'}ba &middot; ${deal.parsed_sqft ? Number(deal.parsed_sqft).toLocaleString() + 'sf' : '?'}<br>
        <a href="${svLink}" target="_blank">Street View</a>
      </div>
    `);
  }

  if (bounds.length > 0) {
    wholesalerDetailMap.fitBounds(bounds, { padding: [30, 30] });
  }
}

// ===== SOURCES =====
let sourcesData = [];
let sourcesCollectors = [];
const LATEST_SCRAPER_VERSION = '1.2.0';

async function loadSources() {
  const container = document.getElementById('sourcesContent');
  if (!container) return;
  if (sourcesData.length === 0) {
    container.innerHTML = `<div class="loading"><div class="spinner"></div><div>Loading sources...</div></div>`;
  }

  try {
    const allPosts = [];
    let offset = 0;
    const batchSize = 1000;
    let keepGoing = true;

    while (keepGoing) {
      const { data, count } = await supabaseGet('fb_deal_posts', {
        select: 'group_name,match_status,captured_at,collector_name,scraper_version',
        order: 'captured_at.desc',
        limit: batchSize,
        offset,
      });
      allPosts.push(...data);
      offset += batchSize;
      if (data.length < batchSize || allPosts.length >= count) keepGoing = false;
    }

    const map = {};
    const collectorVersions = {}; // track latest version per collector
    for (const post of allPosts) {
      const name = post.group_name || 'Unknown';
      if (!map[name]) {
        map[name] = { name, total: 0, matched: 0, multi: 0, noMatch: 0, pending: 0, confirmed: 0, firstSeen: null, lastSeen: null, collectors: new Set() };
      }
      const s = map[name];
      s.total++;
      if (post.match_status === 'matched') s.matched++;
      else if (post.match_status === 'multi_match') s.multi++;
      else if (post.match_status === 'no_match') s.noMatch++;
      else if (post.match_status === 'pending') s.pending++;
      else if (post.match_status === 'confirmed') s.confirmed++;
      if (post.collector_name) s.collectors.add(post.collector_name);
      const d = new Date(post.captured_at);
      if (!s.firstSeen || d < s.firstSeen) s.firstSeen = d;
      if (!s.lastSeen || d > s.lastSeen) s.lastSeen = d;

      // Track collector versions
      if (post.collector_name) {
        const cn = post.collector_name;
        if (!collectorVersions[cn]) collectorVersions[cn] = { name: cn, version: null, lastActive: null, total: 0 };
        collectorVersions[cn].total++;
        if (post.scraper_version) collectorVersions[cn].version = post.scraper_version;
        if (!collectorVersions[cn].lastActive || d > collectorVersions[cn].lastActive) collectorVersions[cn].lastActive = d;
      }
    }

    // Store collector info for rendering
    sourcesCollectors = Object.values(collectorVersions).sort((a, b) => b.total - a.total);

    sourcesData = Object.values(map).map(s => ({
      ...s,
      collectors: Array.from(s.collectors),
      processed: s.total - s.pending,
      matchRate: (s.total - s.pending) > 0 ? Math.round(((s.matched + s.multi + s.confirmed) / (s.total - s.pending)) * 100) : 0,
    })).sort((a, b) => b.total - a.total);

    renderSources();
  } catch (err) {
    console.error('Failed to load sources:', err);
    container.innerHTML = `<div style="color:var(--red);text-align:center;padding:40px;">Failed to load sources: ${escapeHtml(err.message)}</div>`;
  }
}

function renderSources() {
  const container = document.getElementById('sourcesContent');
  if (!container) return;

  const totalPosts = sourcesData.reduce((s, g) => s + g.total, 0);
  const totalProcessed = sourcesData.reduce((s, g) => s + g.processed, 0);

  container.innerHTML = `
    <div class="kpi-grid" style="grid-template-columns:repeat(4,1fr);margin-bottom:20px;">
      <div class="kpi-card blue">
        <div class="kpi-glow"></div>
        <div class="kpi-value">${sourcesData.length}</div>
        <div class="kpi-label">Active Sources</div>
      </div>
      <div class="kpi-card green">
        <div class="kpi-glow"></div>
        <div class="kpi-value">${totalPosts.toLocaleString()}</div>
        <div class="kpi-label">Total Posts</div>
      </div>
      <div class="kpi-card purple">
        <div class="kpi-glow"></div>
        <div class="kpi-value">${totalProcessed.toLocaleString()}</div>
        <div class="kpi-label">Processed</div>
      </div>
      <div class="kpi-card cyan">
        <div class="kpi-glow"></div>
        <div class="kpi-value">${new Set(sourcesData.flatMap(s => s.collectors)).size}</div>
        <div class="kpi-label">Collectors</div>
      </div>
    </div>
    <table class="deals-table">
      <thead>
        <tr>
          <th>Source Group</th>
          <th>Total Posts</th>
          <th>Processed</th>
          <th>Matched</th>
          <th>Multi</th>
          <th>No Match</th>
          <th>Match Rate</th>
          <th>Date Range</th>
          <th>Collectors</th>
        </tr>
      </thead>
      <tbody>
        ${sourcesData.map(s => {
          const rateColor = s.matchRate >= 40 ? 'var(--green)' : s.matchRate >= 20 ? 'var(--yellow)' : 'var(--red)';
          const fmt = (d) => d ? `${d.getMonth()+1}/${d.getDate()}/${String(d.getFullYear()).slice(2)}` : '—';
          const firstStr = fmt(s.firstSeen);
          const lastStr = fmt(s.lastSeen);
          const daySpan = s.firstSeen && s.lastSeen ? Math.round((s.lastSeen - s.firstSeen) / (1000*60*60*24)) : 0;
          return `
          <tr>
            <td style="font-weight:600;color:var(--accent-hover)">${escapeHtml(s.name)}</td>
            <td style="font-weight:700">${s.total.toLocaleString()}</td>
            <td>${s.processed.toLocaleString()}</td>
            <td style="color:var(--green)">${s.matched}</td>
            <td style="color:var(--yellow)">${s.multi}</td>
            <td style="color:var(--text-light)">${s.noMatch}</td>
            <td>
              <div style="display:flex;align-items:center;gap:8px;">
                <div style="flex:1;height:6px;background:rgba(255,255,255,0.05);border-radius:3px;overflow:hidden;min-width:50px;">
                  <div style="height:100%;width:${s.matchRate}%;background:${rateColor};border-radius:3px;"></div>
                </div>
                <span style="font-weight:600;font-size:12px;color:${rateColor}">${s.matchRate}%</span>
              </div>
            </td>
            <td style="font-size:12px;">
              <div style="color:var(--text);">${firstStr} <span style="color:var(--text-light)">→</span> ${lastStr}</div>
              <div style="color:var(--text-light);font-size:11px;">${daySpan} day${daySpan !== 1 ? 's' : ''} coverage</div>
            </td>
            <td style="color:var(--text-muted);font-size:12px">${s.collectors.join(', ')}</td>
          </tr>`;
        }).join('')}
      </tbody>
    </table>

    ${sourcesCollectors.length > 0 ? `
    <div style="margin-top:24px;">
      <div style="font-size:14px;font-weight:600;margin-bottom:10px;color:var(--text);">Collectors</div>
      <table class="deals-table">
        <thead>
          <tr>
            <th>Collector</th>
            <th>Posts Collected</th>
            <th>Last Active</th>
            <th>Scraper Version</th>
          </tr>
        </thead>
        <tbody>
          ${sourcesCollectors.map(c => {
            const isLatest = c.version === LATEST_SCRAPER_VERSION;
            const versionColor = !c.version ? 'var(--text-muted)' : isLatest ? 'var(--green)' : 'var(--red)';
            const versionLabel = !c.version ? 'Unknown' : c.version;
            const badge = !c.version ? '' : isLatest ? ' ✓' : ' ⚠ Update needed';
            const lastAct = c.lastActive ? new Date(c.lastActive).toLocaleDateString() : '—';
            return \`
            <tr>
              <td style="font-weight:600;">\${escapeHtml(c.name)}</td>
              <td>\${c.total.toLocaleString()}</td>
              <td>\${lastAct}</td>
              <td>
                <span style="font-weight:600;color:\${versionColor}">v\${versionLabel}\${badge}</span>
              </td>
            </tr>\`;
          }).join('')}
        </tbody>
      </table>
    </div>
    ` : ''}
  `;
}

// ===== DEAL FLOW =====
async function loadDealFlow() {
  const container = document.getElementById('flowContent');
  if (!container) return;
  container.innerHTML = `<div class="loading"><div class="spinner"></div><div>Calculating deal flow...</div></div>`;

  try {
    const allPosts = [];
    let offset = 0;
    const batchSize = 1000;
    let keepGoing = true;

    while (keepGoing) {
      const { data, count } = await supabaseGet('fb_deal_posts', {
        select: 'match_status,captured_at,matched_address',
        filters: [{ col: 'match_status', val: 'neq.pending' }],
        order: 'captured_at.asc',
        limit: batchSize,
        offset,
      });
      allPosts.push(...data);
      offset += batchSize;
      if (data.length < batchSize || allPosts.length >= count) keepGoing = false;
    }

    // Deduplicate deals
    const dedupedPosts = deduplicateDeals(allPosts);

    // Group by day
    const dailyMap = {};
    for (const post of dedupedPosts) {
      const d = new Date(post.captured_at);
      const key = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
      if (!dailyMap[key]) dailyMap[key] = { date: key, total: 0, matched: 0, noMatch: 0 };
      dailyMap[key].total++;
      if (post.match_status === 'matched' || post.match_status === 'multi_match' || post.match_status === 'confirmed') {
        dailyMap[key].matched++;
      } else {
        dailyMap[key].noMatch++;
      }
    }

    const days = Object.values(dailyMap).sort((a, b) => a.date.localeCompare(b.date));
    const maxTotal = Math.max(...days.map(d => d.total), 1);

    // Weekly aggregation
    const weeklyMap = {};
    for (const day of days) {
      const d = new Date(day.date);
      const weekStart = new Date(d);
      weekStart.setDate(d.getDate() - d.getDay());
      const key = `${weekStart.getFullYear()}-${String(weekStart.getMonth() + 1).padStart(2, '0')}-${String(weekStart.getDate()).padStart(2, '0')}`;
      if (!weeklyMap[key]) weeklyMap[key] = { week: key, total: 0, matched: 0, noMatch: 0 };
      weeklyMap[key].total += day.total;
      weeklyMap[key].matched += day.matched;
      weeklyMap[key].noMatch += day.noMatch;
    }
    const weeks = Object.values(weeklyMap).sort((a, b) => a.week.localeCompare(b.week));
    const maxWeekly = Math.max(...weeks.map(w => w.total), 1);

    // Total stats
    const totalDeals = dedupedPosts.length;
    const totalMatched = dedupedPosts.filter(p => ['matched', 'multi_match', 'confirmed'].includes(p.match_status)).length;
    const avgDaily = days.length > 0 ? Math.round(totalDeals / days.length) : 0;

    container.innerHTML = `
      <div class="kpi-grid" style="grid-template-columns:repeat(4,1fr);margin-bottom:24px;">
        <div class="kpi-card blue">
          <div class="kpi-glow"></div>
          <div class="kpi-value">${totalDeals.toLocaleString()}</div>
          <div class="kpi-label">Total Processed</div>
        </div>
        <div class="kpi-card green">
          <div class="kpi-glow"></div>
          <div class="kpi-value">${totalMatched.toLocaleString()}</div>
          <div class="kpi-label">Total Matched</div>
        </div>
        <div class="kpi-card purple">
          <div class="kpi-glow"></div>
          <div class="kpi-value">${avgDaily}</div>
          <div class="kpi-label">Avg Daily Volume</div>
        </div>
        <div class="kpi-card cyan">
          <div class="kpi-glow"></div>
          <div class="kpi-value">${days.length}</div>
          <div class="kpi-label">Active Days</div>
        </div>
      </div>

      <div style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);padding:24px;margin-bottom:24px;">
        <div class="section-label" style="margin-bottom:16px;">Weekly Deal Volume</div>
        <div style="display:flex;align-items:flex-end;gap:3px;height:180px;padding-bottom:24px;position:relative;">
          ${weeks.map(w => {
            const matchedH = (w.matched / maxWeekly) * 100;
            const noMatchH = (w.noMatch / maxWeekly) * 100;
            return `
            <div style="flex:1;display:flex;flex-direction:column;justify-content:flex-end;align-items:center;height:100%;position:relative;min-width:0;" title="Week of ${w.week}: ${w.total} posts (${w.matched} matched)">
              <div style="width:100%;max-width:40px;border-radius:3px 3px 0 0;background:var(--green);height:${matchedH}%;min-height:${w.matched > 0 ? 2 : 0}px;opacity:0.8;"></div>
              <div style="width:100%;max-width:40px;background:rgba(255,255,255,0.06);height:${noMatchH}%;min-height:${w.noMatch > 0 ? 2 : 0}px;"></div>
            </div>`;
          }).join('')}
        </div>
        <div style="display:flex;justify-content:space-between;font-size:11px;color:var(--text-light);margin-top:4px;">
          <span>${weeks.length > 0 ? weeks[0].week : ''}</span>
          <span>${weeks.length > 0 ? weeks[weeks.length - 1].week : ''}</span>
        </div>
        <div style="display:flex;gap:16px;margin-top:12px;font-size:11px;color:var(--text-muted);">
          <span><span style="display:inline-block;width:10px;height:10px;border-radius:2px;background:var(--green);opacity:0.8;margin-right:4px;vertical-align:middle;"></span>Matched</span>
          <span><span style="display:inline-block;width:10px;height:10px;border-radius:2px;background:rgba(255,255,255,0.06);margin-right:4px;vertical-align:middle;"></span>No Match</span>
        </div>
      </div>

      <div style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);padding:24px;">
        <div class="section-label" style="margin-bottom:16px;">Daily Breakdown</div>
        <div style="max-height:300px;overflow-y:auto;">
          <table class="deals-table" style="margin-top:0;">
            <thead>
              <tr>
                <th>Date</th>
                <th>Total</th>
                <th>Matched</th>
                <th>No Match</th>
                <th>Match Rate</th>
              </tr>
            </thead>
            <tbody>
              ${[...days].reverse().map(d => {
                const rate = d.total > 0 ? Math.round((d.matched / d.total) * 100) : 0;
                const rateColor = rate >= 40 ? 'var(--green)' : rate >= 20 ? 'var(--yellow)' : 'var(--red)';
                return `
                <tr>
                  <td style="font-weight:500">${d.date}</td>
                  <td style="font-weight:700">${d.total}</td>
                  <td style="color:var(--green)">${d.matched}</td>
                  <td style="color:var(--text-light)">${d.noMatch}</td>
                  <td>
                    <div style="display:flex;align-items:center;gap:8px;">
                      <div style="flex:1;height:4px;background:rgba(255,255,255,0.05);border-radius:2px;overflow:hidden;min-width:40px;">
                        <div style="height:100%;width:${rate}%;background:${rateColor};border-radius:2px;"></div>
                      </div>
                      <span style="font-size:12px;font-weight:600;color:${rateColor}">${rate}%</span>
                    </div>
                  </td>
                </tr>`;
              }).join('')}
            </tbody>
          </table>
        </div>
      </div>
    `;
  } catch (err) {
    console.error('Failed to load deal flow:', err);
    container.innerHTML = `<div style="color:var(--red);text-align:center;padding:40px;">Failed to load deal flow: ${escapeHtml(err.message)}</div>`;
  }
}

// ===== MARKETS =====
async function loadMarkets() {
  const container = document.getElementById('marketsContent');
  if (!container) return;
  container.innerHTML = `<div class="loading"><div class="spinner"></div><div>Analyzing markets...</div></div>`;

  try {
    const allPosts = [];
    let offset = 0;
    const batchSize = 1000;
    let keepGoing = true;

    while (keepGoing) {
      const { data, count } = await supabaseGet('fb_deal_posts', {
        select: 'parsed_city,parsed_state,parsed_zip,match_status,parsed_asking_price,parsed_arv,matched_address,captured_at',
        filters: [{ col: 'match_status', val: 'in.(matched,multi_match,no_match)' }],
        order: 'captured_at.desc',
        limit: batchSize,
        offset,
      });
      allPosts.push(...data);
      offset += batchSize;
      if (data.length < batchSize || allPosts.length >= count) keepGoing = false;
    }

    // Deduplicate deals
    const dedupedPosts = deduplicateDeals(allPosts);

    // Aggregate by city+state - exclude junk/pending, only real deal posts
    // Match rate = unique matched addresses / unique total posts
    const cityMap = {};
    const stateMap = {};
    for (const post of dedupedPosts) {
      const city = post.parsed_city;
      const state = post.parsed_state;
      if (!city || !state || city.toLowerCase() === 'unknown') continue;
      const cityKey = `${city}, ${state}`;

      if (!cityMap[cityKey]) {
        cityMap[cityKey] = { city, state, key: cityKey, total: 0, matchedAddrs: new Set(), prices: [], gmv: 0 };
      }
      cityMap[cityKey].total++;
      if (['matched', 'multi_match'].includes(post.match_status) && post.matched_address) {
        cityMap[cityKey].matchedAddrs.add(post.matched_address.toUpperCase());
      }
      if (post.parsed_asking_price) cityMap[cityKey].prices.push(Number(post.parsed_asking_price));
      if (['matched', 'multi_match'].includes(post.match_status)) {
        cityMap[cityKey].gmv += Number(post.parsed_arv) || Number(post.parsed_asking_price) || 0;
      }

      if (!stateMap[state]) {
        stateMap[state] = { state, total: 0, matchedAddrs: new Set(), cities: new Set() };
      }
      stateMap[state].total++;
      if (['matched', 'multi_match'].includes(post.match_status) && post.matched_address) {
        stateMap[state].matchedAddrs.add(post.matched_address.toUpperCase());
      }
      stateMap[state].cities.add(city);
    }

    const cities = Object.values(cityMap).map(c => ({
      ...c,
      uniqueMatched: c.matchedAddrs.size,
      matchRate: c.total > 0 ? Math.round((c.matchedAddrs.size / c.total) * 100) : 0,
      avgPrice: c.prices.length > 0 ? Math.round(c.prices.reduce((a, b) => a + b, 0) / c.prices.length) : 0,
    })).sort((a, b) => b.total - a.total);

    const states = Object.values(stateMap).map(s => ({
      ...s,
      cityCount: s.cities.size,
      uniqueMatched: s.matchedAddrs.size,
      matchRate: s.total > 0 ? Math.round((s.matchedAddrs.size / s.total) * 100) : 0,
    })).sort((a, b) => b.total - a.total);

    const topCity = cities[0];

    container.innerHTML = `
      <div class="kpi-grid" style="grid-template-columns:repeat(4,1fr);margin-bottom:24px;">
        <div class="kpi-card blue">
          <div class="kpi-glow"></div>
          <div class="kpi-value">${states.length}</div>
          <div class="kpi-label">States</div>
        </div>
        <div class="kpi-card green">
          <div class="kpi-glow"></div>
          <div class="kpi-value">${cities.length}</div>
          <div class="kpi-label">Cities</div>
        </div>
        <div class="kpi-card purple">
          <div class="kpi-glow"></div>
          <div class="kpi-value">${topCity ? escapeHtml(topCity.city) : '-'}</div>
          <div class="kpi-label">Top Market</div>
        </div>
        <div class="kpi-card cyan">
          <div class="kpi-glow"></div>
          <div class="kpi-value">${topCity ? topCity.total : 0}</div>
          <div class="kpi-label">Top Market Deals</div>
        </div>
      </div>

      <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;">
        <div style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);padding:24px;">
          <div class="section-label" style="margin-bottom:12px;">Top Cities</div>
          <div style="max-height:400px;overflow-y:auto;">
            <table class="deals-table" style="margin-top:0;">
              <thead>
                <tr>
                  <th>City</th>
                  <th>Posts</th>
                  <th>Matched</th>
                  <th>Match Rate</th>
                  <th>Avg Price</th>
                </tr>
              </thead>
              <tbody>
                ${cities.slice(0, 30).map(c => {
                  const rateColor = c.matchRate >= 40 ? 'var(--green)' : c.matchRate >= 20 ? 'var(--yellow)' : 'var(--red)';
                  return `
                  <tr>
                    <td style="font-weight:600;color:var(--accent-hover)">${escapeHtml(c.key)}</td>
                    <td style="font-weight:700">${c.total}</td>
                    <td style="color:var(--green)">${c.uniqueMatched}</td>
                    <td>
                      <div style="display:flex;align-items:center;gap:6px;">
                        <div style="flex:1;height:4px;background:rgba(255,255,255,0.05);border-radius:2px;overflow:hidden;min-width:30px;">
                          <div style="height:100%;width:${c.matchRate}%;background:${rateColor};border-radius:2px;"></div>
                        </div>
                        <span style="font-size:11px;font-weight:600;color:${rateColor}">${c.matchRate}%</span>
                      </div>
                    </td>
                    <td style="color:var(--text-muted)">${c.avgPrice ? '$' + c.avgPrice.toLocaleString() : '-'}</td>
                  </tr>`;
                }).join('')}
              </tbody>
            </table>
          </div>
        </div>

        <div style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);padding:24px;">
          <div class="section-label" style="margin-bottom:12px;">States Overview</div>
          <div style="max-height:400px;overflow-y:auto;">
            <table class="deals-table" style="margin-top:0;">
              <thead>
                <tr>
                  <th>State</th>
                  <th>Deals</th>
                  <th>Cities</th>
                  <th>Match Rate</th>
                </tr>
              </thead>
              <tbody>
                ${states.map(s => {
                  const rateColor = s.matchRate >= 40 ? 'var(--green)' : s.matchRate >= 20 ? 'var(--yellow)' : 'var(--red)';
                  return `
                  <tr>
                    <td style="font-weight:600">${escapeHtml(s.state)}</td>
                    <td style="font-weight:700">${s.total}</td>
                    <td style="color:var(--text-muted)">${s.cityCount}</td>
                    <td>
                      <div style="display:flex;align-items:center;gap:6px;">
                        <div style="flex:1;height:4px;background:rgba(255,255,255,0.05);border-radius:2px;overflow:hidden;min-width:30px;">
                          <div style="height:100%;width:${s.matchRate}%;background:${rateColor};border-radius:2px;"></div>
                        </div>
                        <span style="font-size:11px;font-weight:600;color:${rateColor}">${s.matchRate}%</span>
                      </div>
                    </td>
                  </tr>`;
                }).join('')}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    `;
  } catch (err) {
    console.error('Failed to load markets:', err);
    container.innerHTML = `<div style="color:var(--red);text-align:center;padding:40px;">Failed to load markets: ${escapeHtml(err.message)}</div>`;
  }
}

// ===== RENDERING =====
function formatDate(dateStr) {
  if (!dateStr) return '-';
  const d = new Date(dateStr);
  const mo = d.getMonth() + 1;
  const dy = d.getDate();
  const yr = String(d.getFullYear()).slice(2);
  let hr = d.getHours();
  const mn = String(d.getMinutes()).padStart(2, '0');
  const ampm = hr >= 12 ? 'PM' : 'AM';
  hr = hr % 12 || 12;
  return `${mo}/${dy}/${yr} ${hr}:${mn}${ampm}`;
}

function formatPrice(val) {
  if (!val) return '-';
  return '$' + Number(val).toLocaleString();
}

function escapeHtml(str) {
  const div = document.createElement('div');
  div.textContent = str || '';
  return div.innerHTML;
}

function getBestAddress(deal) {
  if (deal.matched_address) return deal.matched_address;
  if (deal.parsed_full_address) return deal.parsed_full_address;
  return null;
}

function buildFullAddress(deal) {
  // matched_address is already normalized by backfillPropertyData (street, city, state zip)
  if (deal.matched_address) return deal.matched_address.toUpperCase();
  // Fallback: construct from parsed fields
  const street = getBestAddress(deal);
  if (!street) return '-';
  const city = deal.parsed_city || '';
  const state = deal.parsed_state || '';
  const zip = deal.parsed_zip || '';
  const parts = [street.toUpperCase()];
  if (city) parts.push(city);
  const stateZip = [state, zip].filter(Boolean).join(' ');
  if (stateZip) parts.push(stateZip);
  return parts.join(', ');
}

function addressColor(deal) {
  if (deal.match_status === 'matched' || deal.match_status === 'confirmed') return 'var(--green)';
  if (deal.match_status === 'multi_match') return 'var(--yellow)';
  if (deal.match_status === 'no_match') return 'var(--red)';
  return 'var(--text-light)';
}

function parsedAddress(deal) {
  if (deal.parsed_full_address) return deal.parsed_full_address;
  const street = deal.parsed_street_name || '?';
  const city = deal.parsed_city || '?';
  const state = deal.parsed_state || '?';
  const zip = deal.parsed_zip || '?';
  return `${street}, ${city}, ${state} ${zip}`;
}

function renderDealsToTable(deals, tbodyId) {
  const tbody = document.getElementById(tbodyId);
  tbody.innerHTML = '';

  for (const deal of deals) {
    const tr = document.createElement('tr');
    if (deal.match_status === 'no_match') tr.classList.add('row-no-match');

    const addrColor = addressColor(deal);
    tr.innerHTML = `
      <td>${formatDate(deal.captured_at)}</td>
      <td style="max-width:140px;overflow:hidden;text-overflow:ellipsis">${escapeHtml(deal.group_name || '-')}</td>
      <td>${escapeHtml(parsedAddress(deal))}</td>
      <td style="font-weight:600;color:${addrColor}">${escapeHtml(buildFullAddress(deal))}</td>
      <td>${deal.parsed_beds || '?'}/${deal.parsed_baths || '?'} &middot; ${deal.parsed_sqft ? Number(deal.parsed_sqft).toLocaleString() + 'sf' : '?'}</td>
      <td>${formatPrice(deal.parsed_asking_price)}</td>
      <td><span class="badge badge-${deal.match_status}">${(deal.match_status || '').replace('_', ' ')}</span></td>
      <td style="text-align:center;font-weight:600;color:${deal.match_count === 1 ? 'var(--green)' : deal.match_count > 1 ? 'var(--yellow)' : 'var(--text-light)'}">${deal.match_count || '-'}</td>
      <td>${deal.match_confidence ? `<span class="badge badge-${deal.match_confidence}">${deal.match_confidence}</span>` : '-'}</td>
    `;

    tr.addEventListener('click', () => toggleDetail(deal, tr));
    tbody.appendChild(tr);
  }
}

function toggleDetail(deal, tr) {
  const existingPanel = tr.nextElementSibling;
  if (existingPanel && existingPanel.classList.contains('detail-row')) {
    existingPanel.remove();
    return;
  }

  document.querySelectorAll('.detail-row').forEach(el => el.remove());

  const detailRow = document.createElement('tr');
  detailRow.classList.add('detail-row');
  const td = document.createElement('td');
  td.colSpan = 9;

  const candidates = deal.match_candidates || [];

  const candidatesHtml = candidates.length > 0
    ? `<ul class="candidates-list">${candidates.map((c, i) => {
        const fullAddr = [
          c.property_address_full || '',
          c.property_address_city || '',
          (c.property_address_state || '') + ' ' + (c.property_address_zip || '')
        ].filter(Boolean).join(', ');
        const mapsLink = `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(fullAddr)}`;
        const svImg = `https://maps.googleapis.com/maps/api/streetview?size=200x120&location=${encodeURIComponent(fullAddr)}&key=${GMAPS_KEY}`;
        return `
        <li>
          <a class="addr" href="${mapsLink}" target="_blank" onclick="event.stopPropagation();">${escapeHtml(c.property_address_full || 'Unknown')} &#8599;</a>
          <div class="candidate-detail">
            <div class="streetview-thumb" onclick="event.stopPropagation(); openCompare('${deal.id}', ${i});">
              <img src="${svImg}" alt="Street View" loading="lazy">
            </div>
            <div class="meta">
              ${c.area_building || '?'}sf &middot;
              ${c.bedrooms_count || '?'}bd/${c.bath_count || '?'}ba &middot;
              Built ${c.year_built || '?'}<br>
              Lot ${c.area_lot_sf ? (Math.round(c.area_lot_sf / 43560 * 100) / 100) + 'ac' : '?'}
              ${c._score ? ' &middot; Score: ' + c._score : ''}
              ${deal.match_status === 'multi_match' ? `<br><button class="btn" onclick="event.stopPropagation(); confirmMatch('${deal.id}', ${i})" style="margin-top:6px;background:var(--green);color:#fff;font-size:11px;padding:4px 10px;">✓ Confirm</button>` : ''}
            </div>
          </div>
        </li>`;
      }).join('')}</ul>`
    : '<p style="color:var(--text-light);font-size:13px;">No candidates found</p>';

  const images = deal.post_images || [];
  const imagesHtml = images.length > 0
    ? `<div class="detail-images">${images.map((url, i) => `<img src="${url}" alt="property photo" onclick="event.stopPropagation(); openLightbox('${deal.id}', ${i})">`).join('')}</div>`
    : '';

  td.innerHTML = `
    <div class="detail-panel">
      <div class="detail-row-layout">
        <div class="detail-subject">
          <div class="section-label">Post Text</div>
          <pre>${escapeHtml(deal.post_text)}</pre>
          ${imagesHtml}
          <div class="detail-poster">
            Posted by: ${escapeHtml(deal.poster_name || 'Unknown')}
            ${deal.collector_name ? ` &middot; Collector: ${escapeHtml(deal.collector_name)}` : ''}
            ${deal.post_url ? ` &middot; <a href="${deal.post_url}" target="_blank" onclick="event.stopPropagation();">View Post</a>` : ''}
          </div>
        </div>
        <div class="detail-candidates">
          <div class="section-label">Match Candidates (${candidates.length})</div>
          ${candidatesHtml}
        </div>
      </div>
    </div>
  `;

  detailRow.appendChild(td);
  tr.after(detailRow);
}

// ===== PAGINATION =====
function renderPaginationTo(elementId, count, page) {
  const totalPages = Math.ceil(count / PAGE_SIZE) || 1;
  const displayPage = page + 1;
  const el = document.getElementById(elementId);

  const isDash = elementId === 'dashPagination';
  const prevAction = isDash ? `goPageDash(${page - 1})` : `goPage(${page - 1})`;
  const nextAction = isDash ? `goPageDash(${page + 1})` : `goPage(${page + 1})`;

  el.innerHTML = `
    <button ${displayPage <= 1 ? 'disabled' : ''} onclick="${prevAction}">&larr; Prev</button>
    <span class="page-info">Page ${displayPage} of ${totalPages} &middot; ${count.toLocaleString()} deals</span>
    <button ${displayPage >= totalPages ? 'disabled' : ''} onclick="${nextAction}">Next &rarr;</button>
  `;
}

function goPage(page) {
  currentPage = page;
  loadDeals();
}

function goPageDash(page) {
  currentPage = page;
  loadDashboardDeals();
}

// ===== LIGHTBOX =====
let lightboxImages = [];
let lightboxIndex = 0;
let lightboxEl = null;

function openLightbox(dealId, index) {
  const deal = allDeals.find(d => d.id === dealId);
  if (!deal || !deal.post_images || deal.post_images.length === 0) return;

  lightboxImages = deal.post_images;
  lightboxIndex = index;

  if (lightboxEl) lightboxEl.remove();

  lightboxEl = document.createElement('div');
  lightboxEl.className = 'lightbox';
  lightboxEl.innerHTML = `
    <button class="lightbox-close" onclick="closeLightbox()">&times;</button>
    ${lightboxImages.length > 1 ? `<button class="lightbox-nav lightbox-prev" onclick="lightboxNav(-1)">&#8249;</button>` : ''}
    <img src="${lightboxImages[index]}" alt="Property photo">
    ${lightboxImages.length > 1 ? `<button class="lightbox-nav lightbox-next" onclick="lightboxNav(1)">&#8250;</button>` : ''}
    ${lightboxImages.length > 1 ? `<div class="lightbox-counter">${index + 1} / ${lightboxImages.length}</div>` : ''}
  `;
  lightboxEl.addEventListener('click', (e) => {
    if (e.target === lightboxEl) closeLightbox();
  });
  document.body.appendChild(lightboxEl);
  requestAnimationFrame(() => lightboxEl.classList.add('open'));
  document.addEventListener('keydown', lightboxKeyHandler);
}

function closeLightbox() {
  if (lightboxEl) {
    lightboxEl.classList.remove('open');
    setTimeout(() => { if (lightboxEl) { lightboxEl.remove(); lightboxEl = null; } }, 200);
  }
  document.removeEventListener('keydown', lightboxKeyHandler);
}

function lightboxNav(dir) {
  lightboxIndex = (lightboxIndex + dir + lightboxImages.length) % lightboxImages.length;
  if (lightboxEl) {
    lightboxEl.querySelector('img').src = lightboxImages[lightboxIndex];
    const counter = lightboxEl.querySelector('.lightbox-counter');
    if (counter) counter.textContent = `${lightboxIndex + 1} / ${lightboxImages.length}`;
  }
}

function lightboxKeyHandler(e) {
  if (e.key === 'Escape') closeLightbox();
  if (e.key === 'ArrowLeft') lightboxNav(-1);
  if (e.key === 'ArrowRight') lightboxNav(1);
}

// ===== COMPARE VIEWER =====
let compareEl = null;
let compareDealId = null;
let compareCandidateIndex = 0;

function openCompare(dealId, candidateIndex) {
  compareDealId = dealId;
  compareCandidateIndex = candidateIndex;
  renderCompare();
}

function renderCompare() {
  const deal = allDeals.find(d => d.id === compareDealId);
  if (!deal) return;

  const candidates = deal.match_candidates || [];
  const candidate = candidates[compareCandidateIndex];
  if (!candidate) return;

  const subjectImg = (deal.post_images && deal.post_images.length > 0) ? deal.post_images[0] : null;

  const fullAddr = [
    candidate.property_address_full || '',
    candidate.property_address_city || '',
    (candidate.property_address_state || '') + ' ' + (candidate.property_address_zip || '')
  ].filter(Boolean).join(', ');
  const svImg = `https://maps.googleapis.com/maps/api/streetview?size=800x500&location=${encodeURIComponent(fullAddr)}&key=${GMAPS_KEY}`;
  const mapsLink = `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(fullAddr)}`;

  const hasPrev = compareCandidateIndex > 0;
  const hasNext = compareCandidateIndex < candidates.length - 1;

  if (compareEl) compareEl.remove();

  compareEl = document.createElement('div');
  compareEl.className = 'compare-overlay';
  compareEl.innerHTML = `
    <div class="compare-panel">
      <button class="compare-close" onclick="closeCompare()">&times;</button>
      ${hasPrev ? '<button class="compare-nav compare-prev" onclick="compareNav(-1)">&#8249;</button>' : ''}
      ${hasNext ? '<button class="compare-nav compare-next" onclick="compareNav(1)">&#8250;</button>' : ''}
      <div class="compare-header">
        <div class="compare-title">Subject Property</div>
        <div class="compare-title">
          <a href="${mapsLink}" target="_blank" style="color:var(--accent-hover);text-decoration:none;">${escapeHtml(candidate.property_address_full || 'Unknown')} &#8599;</a>
          <span style="font-size:12px;color:var(--text-muted);font-weight:400;margin-left:8px;">
            ${candidate.area_building || '?'}sf &middot; ${candidate.bedrooms_count || '?'}bd/${candidate.bath_count || '?'}ba &middot; Built ${candidate.year_built || '?'} &middot; Lot ${candidate.area_lot_sf ? (Math.round(candidate.area_lot_sf / 43560 * 100) / 100) + 'ac' : '?'}
          </span>
        </div>
      </div>
      <div class="compare-counter">${compareCandidateIndex + 1} of ${candidates.length} candidates</div>
      <div class="compare-images">
        <div class="compare-side">
          ${subjectImg
            ? `<img src="${subjectImg}" alt="Subject property">`
            : '<div class="compare-no-img">No subject photo</div>'}
        </div>
        <div class="compare-side">
          <img src="${svImg}" alt="Street View">
        </div>
      </div>
      ${deal.post_images && deal.post_images.length > 1 ? `
        <div class="compare-thumbs">
          ${deal.post_images.map((url, i) => `<img src="${url}" class="${i === 0 ? 'active' : ''}" onclick="event.stopPropagation(); swapSubjectImg(this, '${url}')">`).join('')}
        </div>
      ` : ''}
      <div style="display:flex;justify-content:center;gap:12px;margin-top:16px;padding-top:16px;border-top:1px solid var(--border);">
        <button id="confirmMatchBtn" class="btn" onclick="event.stopPropagation(); confirmMatch('${dealId}', ${compareCandidateIndex})" style="background:var(--green);color:#fff;font-weight:600;padding:10px 24px;font-size:14px;">
          ✓ Confirm This Match
        </button>
        <button class="btn" onclick="event.stopPropagation(); rejectMatch('${dealId}')" style="background:var(--red);color:#fff;font-weight:600;padding:10px 24px;font-size:14px;">
          ✗ No Match
        </button>
      </div>
    </div>
  `;
  compareEl.addEventListener('click', (e) => {
    if (e.target === compareEl) closeCompare();
  });
  document.body.appendChild(compareEl);
  requestAnimationFrame(() => compareEl.classList.add('open'));
  document.addEventListener('keydown', compareKeyHandler);
}

function compareNav(dir) {
  const deal = allDeals.find(d => d.id === compareDealId);
  if (!deal) return;
  const candidates = deal.match_candidates || [];
  const newIndex = compareCandidateIndex + dir;
  if (newIndex < 0 || newIndex >= candidates.length) return;
  compareCandidateIndex = newIndex;
  renderCompare();
}

function closeCompare() {
  if (compareEl) {
    compareEl.classList.remove('open');
    setTimeout(() => { if (compareEl) { compareEl.remove(); compareEl = null; } }, 200);
  }
  document.removeEventListener('keydown', compareKeyHandler);
}

function swapSubjectImg(thumbEl, url) {
  const panel = thumbEl.closest('.compare-panel');
  const mainImg = panel.querySelector('.compare-side:first-child img');
  if (mainImg) mainImg.src = url;
  panel.querySelectorAll('.compare-thumbs img').forEach(t => t.classList.remove('active'));
  thumbEl.classList.add('active');
}

function compareKeyHandler(e) {
  if (e.key === 'Escape') closeCompare();
  if (e.key === 'ArrowLeft') compareNav(-1);
  if (e.key === 'ArrowRight') compareNav(1);
}

// ===== MATCH CONFIRMATION =====
async function confirmMatch(dealId, candidateIndex) {
  const deal = allDeals.find(d => d.id === dealId) || wholesalerDeals.find(d => d.id === dealId);
  if (!deal) return;
  const candidate = deal.match_candidates?.[candidateIndex];
  if (!candidate) return;

  const btn = document.getElementById('confirmMatchBtn');
  if (btn) { btn.textContent = 'Saving...'; btn.disabled = true; }

  try {
    // Build normalized address from the confirmed candidate
    const street = candidate.property_address_full || deal.matched_address || '';
    const city = candidate.property_address_city || deal.parsed_city || '';
    const state = candidate.property_address_state || deal.parsed_state || '';
    const zip = candidate.property_address_zip || deal.parsed_zip || '';
    const parts = [street.trim()];
    if (city) parts.push(city.trim());
    const stateZip = [state.trim(), zip.trim()].filter(Boolean).join(' ');
    if (stateZip) parts.push(stateZip);
    const normalizedAddr = parts.filter(Boolean).join(', ');

    await supabasePatch('fb_deal_posts', dealId, {
      match_status: 'confirmed',
      matched_address: normalizedAddr,
      match_count: 1,
      match_confidence: 'manual',
      match_candidates: [candidate],
    });

    // Update local data
    deal.match_status = 'confirmed';
    deal.matched_address = normalizedAddr;
    deal.match_count = 1;
    deal.match_confidence = 'manual';
    deal.match_candidates = [candidate];

    closeCompare();
    // Refresh the current view
    document.querySelectorAll('.detail-row').forEach(el => el.remove());
    if (typeof loadDeals === 'function' && document.getElementById('view-deals')?.style.display !== 'none') loadDeals();
    if (typeof loadDashboardDeals === 'function' && document.getElementById('view-dashboard')?.style.display !== 'none') loadDashboardDeals();
  } catch (err) {
    console.error('Failed to confirm match:', err);
    if (btn) { btn.textContent = 'Error — try again'; btn.disabled = false; btn.style.background = 'var(--red)'; }
  }
}

async function rejectMatch(dealId) {
  const deal = allDeals.find(d => d.id === dealId) || wholesalerDeals.find(d => d.id === dealId);
  if (!deal) return;

  try {
    await supabasePatch('fb_deal_posts', dealId, {
      match_status: 'no_match',
      matched_address: null,
      match_count: 0,
      match_confidence: null,
    });

    deal.match_status = 'no_match';
    deal.matched_address = null;
    deal.match_count = 0;
    deal.match_confidence = null;

    closeCompare();
    document.querySelectorAll('.detail-row').forEach(el => el.remove());
    if (typeof loadDeals === 'function' && document.getElementById('view-deals')?.style.display !== 'none') loadDeals();
    if (typeof loadDashboardDeals === 'function' && document.getElementById('view-dashboard')?.style.display !== 'none') loadDashboardDeals();
  } catch (err) {
    console.error('Failed to reject match:', err);
    alert('Failed to reject match: ' + err.message);
  }
}

// ===== EVENT LISTENERS =====
document.getElementById('statusFilter').addEventListener('change', (e) => {
  currentFilter = e.target.value;
  currentPage = 0;
  loadDeals();
});

// ===== SETTINGS =====
function openSettings() {
  const overlay = document.createElement('div');
  overlay.className = 'compare-overlay';
  overlay.id = 'settingsOverlay';
  overlay.innerHTML = `
    <div class="compare-panel" style="max-width:480px;width:100%">
      <button class="compare-close" onclick="document.getElementById('settingsOverlay').remove()">&times;</button>
      <h2 style="margin-bottom:4px;font-size:18px;font-weight:700;">Settings</h2>
      <p style="font-size:13px;color:var(--text-muted);margin-bottom:20px;">Configure your data connections</p>
      <div style="display:flex;flex-direction:column;gap:16px;">
        <label style="font-size:12px;font-weight:600;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.5px;">
          Supabase URL
          <input type="text" id="setSupabaseUrl" value="${escapeHtml(localStorage.getItem('omr_supabase_url') || '')}" placeholder="https://xxx.supabase.co" style="width:100%;padding:10px 14px;border:1px solid var(--border);border-radius:8px;font-size:13px;font-family:inherit;margin-top:6px;background:var(--surface);color:var(--text);outline:none;">
        </label>
        <label style="font-size:12px;font-weight:600;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.5px;">
          Supabase Anon Key
          <input type="text" id="setSupabaseKey" value="${escapeHtml(localStorage.getItem('omr_supabase_key') || '')}" placeholder="eyJhbG..." style="width:100%;padding:10px 14px;border:1px solid var(--border);border-radius:8px;font-size:13px;font-family:inherit;margin-top:6px;background:var(--surface);color:var(--text);outline:none;">
        </label>
        <label style="font-size:12px;font-weight:600;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.5px;">
          Google Maps API Key <span style="font-weight:400;color:var(--text-light);text-transform:none;">(optional)</span>
          <input type="text" id="setGmapsKey" value="${escapeHtml(localStorage.getItem('omr_gmaps_key') || '')}" placeholder="AIzaSy..." style="width:100%;padding:10px 14px;border:1px solid var(--border);border-radius:8px;font-size:13px;font-family:inherit;margin-top:6px;background:var(--surface);color:var(--text);outline:none;">
        </label>
      </div>
      <div style="display:flex;gap:10px;margin-top:24px;justify-content:flex-end;">
        <button class="btn" onclick="document.getElementById('settingsOverlay').remove()">Cancel</button>
        <button class="btn btn-primary" onclick="saveSettings()">Save & Reload</button>
      </div>
    </div>
  `;
  document.body.appendChild(overlay);
  requestAnimationFrame(() => overlay.classList.add('open'));
}

function saveSettings() {
  localStorage.setItem('omr_supabase_url', document.getElementById('setSupabaseUrl').value.trim());
  localStorage.setItem('omr_supabase_key', document.getElementById('setSupabaseKey').value.trim());
  localStorage.setItem('omr_gmaps_key', document.getElementById('setGmapsKey').value.trim());
  location.reload();
}

// ===== INIT =====
function init() {
  if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
    document.getElementById('dashLoadingState').innerHTML = `
      <div style="max-width:420px;margin:0 auto;text-align:center;">
        <div style="font-size:20px;font-weight:700;margin-bottom:8px;">Welcome to Off Market Radar</div>
        <div style="color:var(--text-muted);margin-bottom:24px;line-height:1.6;">Connect your Supabase database to start tracking off-market deal leads.</div>
        <button class="btn btn-primary" onclick="openSettings()" style="padding:12px 28px;font-size:14px;">Setup Connection</button>
      </div>
    `;
    return;
  }

  // Set dashboard tagline on init
  const headerEl = document.getElementById('headerTitle');
  headerEl.textContent = 'Leveraging AI to create the most comprehensive insight into off market deal flow';
  headerEl.classList.add('tagline');

  loadKPIs();
  loadDashboardDeals();
  loadDashMap();
  const now = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  document.getElementById('lastSync').innerHTML = `<span class="dot"></span><span>Synced ${now}</span>`;
}

init();
