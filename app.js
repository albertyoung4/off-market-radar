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
// Deduplicate deals by matched_address, keeping the record with the highest price value
function deduplicateDeals(deals) {
  const addrMap = new Map();
  const noAddr = [];
  for (const d of deals) {
    const addr = (d.matched_address || '').toUpperCase().trim();
    if (!addr) { noAddr.push(d); continue; }
    const val = Number(d.parsed_arv) || Number(d.parsed_asking_price) || 0;
    const existing = addrMap.get(addr);
    if (!existing) {
      addrMap.set(addr, d);
    } else {
      // Keep whichever has the higher price value
      const existingVal = Number(existing.parsed_arv) || Number(existing.parsed_asking_price) || 0;
      if (val > existingVal) addrMap.set(addr, d);
    }
  }
  return [...addrMap.values(), ...noAddr];
}

// Build a normalized full address from property registry data
function normalizeAddress(deal) {
  const c = deal.match_candidates?.[0] || {};
  // Use registry street address, falling back to matched_address or parsed
  const street = c.property_address_full || deal.matched_address || deal.parsed_full_address || '';
  const city = c.property_address_city || deal.parsed_city || '';
  const state = c.property_address_state || deal.parsed_state || '';
  const zip = c.property_address_zip || deal.parsed_zip || '';

  // If matched_address already has city/state/zip (contains a comma), use as-is
  // Otherwise build complete: "123 Main St, Nashville, TN 37201"
  const trimmedStreet = street.trim();
  if (!trimmedStreet) return '';

  // Check if the street part already contains city/state (has commas)
  if (trimmedStreet.includes(',') && (state || zip)) return trimmedStreet;

  const parts = [trimmedStreet];
  if (city) parts.push(city.trim());
  const stateZip = [state.trim(), zip.trim()].filter(Boolean).join(' ');
  if (stateZip) parts.push(stateZip);
  return parts.filter(Boolean).join(', ');
}

// Backfill beds/baths/sqft and normalize address from match_candidates
function backfillPropertyData(deal) {
  const c = deal.match_candidates?.[0] || {};
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
    transactions: 'Transactions',
    qc: 'Quality Control',
    sources: 'Deal Source Catalog',
    outreach: 'Outreach',
    scraper: 'Download Plugin',
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
  if (view === 'transactions') {
    loadTransactions();
  }
  if (view === 'qc') {
    loadQualityControl();
  }
  if (view === 'outreach') {
    loadOutreach();
  }
  if (view === 'scraper') {
    renderScraperPage();
  }
  if (view === 'team') {
    loadTeamActivity();
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
async function supabaseGet(table, { select = '*', filters = [], order, limit, offset, countMode = 'planned' } = {}) {
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
      'Prefer': `count=${countMode}`,
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

// Fetch ALL rows by paginating in chunks of 1000 (PostgREST caps at 1000 per request)
async function supabaseGetAll(table, { select = '*', filters = [] } = {}) {
  const pageSize = 1000;
  let allData = [];
  let offset = 0;
  let totalCount = 0;
  while (true) {
    const { data, count } = await supabaseGet(table, { select, filters, limit: pageSize, offset });
    if (count) totalCount = count;
    allData = allData.concat(data);
    if (data.length < pageSize) break; // last page
    offset += pageSize;
  }
  return { data: allData, count: totalCount };
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

    // Get status counts + GMV (paginated sum) + wholesaler count in parallel
    const gmvPromise = (async () => {
      // Paginate through ALL matched deals to compute GMV server-side-style
      const allGmvData = [];
      let offset = 0;
      const pageSize = 1000;
      while (true) {
        const { data } = await supabaseGet('fb_deal_posts', {
          select: 'parsed_arv,parsed_asking_price,matched_address',
          filters: [{ col: 'match_status', val: 'in.(matched,multi_match,confirmed)' }],
          limit: pageSize,
          offset,
        });
        allGmvData.push(...data);
        if (data.length < pageSize) break;
        offset += pageSize;
      }
      return allGmvData;
    })();

    const wholesalerPromise = supabaseGetAll('fb_deal_posts', {
      select: 'poster_name',
      filters: [{ col: 'match_status', val: 'neq.pending' }],
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
    const gmvRaw = await gmvPromise;
    const gmvData = deduplicateDeals(gmvRaw);
    let gmv = 0;
    for (const d of gmvData) {
      const val = d.parsed_arv || d.parsed_asking_price;
      if (val) gmv += Number(val);
    }
    const gmvDisplay = gmv >= 1000000 ? '$' + (gmv / 1000000).toFixed(1) + 'M' : gmv >= 1000 ? '$' + (gmv / 1000).toFixed(0) + 'K' : '$' + gmv;

    // Count unique wholesalers (clean poster names)
    const { data: wholesalerData } = await wholesalerPromise;
    const uniqueWholesalers = new Set(wholesalerData.map(d => cleanPosterName(d.poster_name || 'Unknown'))).size;

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
      ${(counts.pending || 0) > 0 ? `
      <div class="kpi-card" style="border-color:var(--yellow);background:rgba(245,158,11,0.08);">
        <div class="kpi-glow"></div>
        <div class="kpi-value" style="color:var(--yellow);">${(counts.pending || 0).toLocaleString()}</div>
        <div class="kpi-label" style="color:var(--yellow);">⚠ Pending Processing</div>
      </div>
      ` : ''}
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
      <div class="kpi-card purple">
        <div class="kpi-glow"></div>
        <div class="kpi-value">${uniqueWholesalers.toLocaleString()}</div>
        <div class="kpi-label">Wholesalers</div>
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

    // Fetch poster contact info (phones/emails) from posters table
    const posterContacts = {};
    try {
      let pOffset = 0;
      let pKeepGoing = true;
      while (pKeepGoing) {
        const { data: pData } = await supabaseGet('posters', {
          select: 'fb_name,phones,emails',
          limit: 1000,
          offset: pOffset,
        });
        for (const p of pData) {
          const cName = cleanPosterName(p.fb_name || '');
          if (!cName) continue;
          if (!posterContacts[cName]) posterContacts[cName] = { phones: [], emails: [] };
          if (p.phones) posterContacts[cName].phones.push(...p.phones);
          if (p.emails) posterContacts[cName].emails.push(...p.emails);
        }
        pOffset += 1000;
        if (pData.length < 1000) pKeepGoing = false;
      }
      // Deduplicate
      for (const k of Object.keys(posterContacts)) {
        posterContacts[k].phones = [...new Set(posterContacts[k].phones)];
        posterContacts[k].emails = [...new Set(posterContacts[k].emails)];
      }
    } catch (e) { console.warn('Could not load poster contacts:', e); }

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

    wholesalersData = Object.values(map).map(w => {
      const contact = posterContacts[w.name] || { phones: [], emails: [] };
      return {
        ...w,
        rawNames: Array.from(w.rawNames),
        groups: Array.from(w.groups),
        phones: contact.phones,
        emails: contact.emails,
        matchRate: w.total > 0 ? Math.round(((w.matched + w.multi + w.confirmed) / w.total) * 100) : 0,
        avgPrice: w.prices.length > 0 ? Math.round(w.prices.reduce((a, b) => a + b, 0) / w.prices.length) : 0,
      };
    });

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
    <table class="deals-table" id="wholesalerTable" style="max-width:1200px;">
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
              <div style="display:flex;align-items:center;gap:5px;">
                ${escapeHtml(w.name)}
                ${w.phones.length > 0 ? '<span title="' + escapeHtml(w.phones.join(', ')) + '" style="color:var(--green);cursor:help;"><svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px;"><path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07 19.5 19.5 0 0 1-6-6 19.79 19.79 0 0 1-3.07-8.67A2 2 0 0 1 4.11 2h3a2 2 0 0 1 2 1.72 12.84 12.84 0 0 0 .7 2.81 2 2 0 0 1-.45 2.11L8.09 9.91a16 16 0 0 0 6 6l1.27-1.27a2 2 0 0 1 2.11-.45 12.84 12.84 0 0 0 2.81.7A2 2 0 0 1 22 16.92z"/></svg></span>' : ''}
                ${w.emails.length > 0 ? '<span title="' + escapeHtml(w.emails.join(', ')) + '" style="color:var(--accent);cursor:help;"><svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px;"><path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><polyline points="22,6 12,13 2,6"/></svg></span>' : ''}
                ${(() => {
                  const ck = 'omr_contact_' + safeBtoa(w.name);
                  const sv = JSON.parse(localStorage.getItem(ck) || '{}');
                  const fbHref = sv.facebook || 'https://www.facebook.com/search/people/?q=' + encodeURIComponent(w.name);
                  const hasFb = !!sv.facebook;
                  return '<a href="' + fbHref + '" target="_blank" onclick="event.stopPropagation();" style="color:' + (hasFb ? 'var(--accent)' : 'var(--text-light)') + ';opacity:' + (hasFb ? '1' : '0.5') + ';transition:opacity 0.15s;" onmouseover="this.style.opacity=1" onmouseout="this.style.opacity=' + (hasFb ? '1' : '0.5') + '" title="' + (hasFb ? 'View Facebook Profile' : 'Search on Facebook') + '">' +
                    '<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px;"><path d="M18 2h-3a5 5 0 0 0-5 5v3H7v4h3v8h4v-8h3l1-4h-4V7a1 1 0 0 1 1-1h3z"/></svg>' +
                  '</a>';
                })()}
              </div>
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
const LATEST_SCRAPER_VERSION = '1.6.1';

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
        select: 'group_name,match_status,captured_at,collector_name,scraper_version,post_timestamp',
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
        map[name] = { name, total: 0, matched: 0, multi: 0, noMatch: 0, pending: 0, confirmed: 0, firstSeen: null, lastSeen: null, earliestPost: null, latestPost: null, withTimestamp: 0, collectors: new Set() };
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

      // Track post timestamps (actual post date, not scrape date)
      if (post.post_timestamp) {
        const pt = new Date(post.post_timestamp);
        if (!isNaN(pt.getTime())) {
          s.withTimestamp++;
          if (!s.earliestPost || pt < s.earliestPost) s.earliestPost = pt;
          if (!s.latestPost || pt > s.latestPost) s.latestPost = pt;
        }
      }

      // Track collector versions
      if (post.collector_name) {
        const cn = post.collector_name;
        if (!collectorVersions[cn]) collectorVersions[cn] = { name: cn, version: null, lastActive: null, total: 0, withTimestamp: 0 };
        collectorVersions[cn].total++;
        if (post.post_timestamp) collectorVersions[cn].withTimestamp++;
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
  const totalWithTimestamp = sourcesData.reduce((s, g) => s + (g.withTimestamp || 0), 0);
  const tsCoverage = totalPosts > 0 ? Math.round((totalWithTimestamp / totalPosts) * 100) : 0;

  container.innerHTML = `
    <div class="kpi-grid" style="grid-template-columns:repeat(5,1fr);margin-bottom:20px;">
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
      <div class="kpi-card ${tsCoverage >= 50 ? 'green' : tsCoverage > 0 ? 'yellow' : 'red'}">
        <div class="kpi-glow"></div>
        <div class="kpi-value">${tsCoverage}%</div>
        <div class="kpi-label">Post Dates Captured</div>
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
          <th>Post Dates</th>
          <th>Scraped Range</th>
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
          // Post date range (actual post dates, not scrape dates)
          const epStr = fmt(s.earliestPost);
          const lpStr = fmt(s.latestPost);
          const postDaySpan = s.earliestPost && s.latestPost ? Math.round((s.latestPost - s.earliestPost) / (1000*60*60*24)) : 0;
          const tsPct = s.total > 0 ? Math.round((s.withTimestamp / s.total) * 100) : 0;
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
              ${s.withTimestamp > 0 ? `
                <div style="color:var(--text);">${epStr} <span style="color:var(--text-light)">→</span> ${lpStr}</div>
                <div style="color:var(--text-light);font-size:11px;">${postDaySpan}d span · ${tsPct}% dated</div>
              ` : `
                <div style="color:var(--text-muted);font-size:11px;">No post dates</div>
                <div style="color:var(--text-muted);font-size:10px;">Needs v1.6.1+</div>
              `}
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
            <th>Post Dates Captured</th>
            <th>Last Active</th>
            <th>Scraper Version</th>
          </tr>
        </thead>
        <tbody>
          ${sourcesCollectors.map(function(c) {
            var isLatest = c.version === LATEST_SCRAPER_VERSION;
            var vColor = !c.version ? 'var(--text-muted)' : isLatest ? 'var(--green)' : 'var(--red)';
            var vLabel = !c.version ? 'Unknown' : c.version;
            var vBadge = !c.version ? '' : isLatest ? ' ✓' : ' ⚠ Update needed';
            var lastAct = c.lastActive ? new Date(c.lastActive).toLocaleDateString() : '—';
            var tsPct = c.total > 0 ? Math.round((c.withTimestamp / c.total) * 100) : 0;
            var tsColor = tsPct >= 80 ? 'var(--green)' : tsPct > 0 ? 'var(--yellow)' : 'var(--text-muted)';
            return '<tr>' +
              '<td style="font-weight:600;">' + escapeHtml(c.name) + '</td>' +
              '<td>' + c.total.toLocaleString() + '</td>' +
              '<td><span style="color:' + tsColor + ';font-weight:600;">' + tsPct + '%</span> <span style="color:var(--text-muted);font-size:11px;">(' + c.withTimestamp + '/' + c.total + ')</span></td>' +
              '<td>' + lastAct + '</td>' +
              '<td><span style="font-weight:600;color:' + vColor + '">v' + vLabel + vBadge + '</span></td>' +
            '</tr>';
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

// ===== SCRAPER DOWNLOAD =====
function renderScraperPage() {
  const container = document.getElementById('scraperContent');
  if (!container) return;

  container.innerHTML = `
    <div style="max-width:640px;margin:0 auto;padding:40px 20px;">
      <div style="text-align:center;margin-bottom:40px;">
        <div style="width:80px;height:80px;border-radius:20px;background:linear-gradient(135deg,var(--accent),var(--purple));display:flex;align-items:center;justify-content:center;margin:0 auto 20px;">
          <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
        </div>
        <h2 style="font-size:24px;font-weight:700;margin-bottom:8px;">Off Market Radar Scraper</h2>
        <p style="color:var(--text-muted);font-size:14px;line-height:1.6;">Chrome extension that captures off-market deal posts from Facebook groups as you browse.</p>
      </div>

      <div style="background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:24px;margin-bottom:24px;">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;">
          <div>
            <div style="font-size:18px;font-weight:700;">Latest Version</div>
            <div style="color:var(--green);font-weight:600;font-size:14px;margin-top:4px;">v${LATEST_SCRAPER_VERSION}</div>
            <div id="zipTimestamp" style="color:var(--text-muted);font-size:12px;margin-top:2px;">Loading build date...</div>
          </div>
          <a href="fb-deal-scraper-latest.zip" download="fb-deal-scraper-v${LATEST_SCRAPER_VERSION}.zip" style="display:inline-flex;align-items:center;gap:8px;padding:12px 28px;border-radius:8px;background:var(--accent);color:#fff;text-decoration:none;font-size:14px;font-weight:600;transition:opacity 0.15s;" onmouseover="this.style.opacity=0.85" onmouseout="this.style.opacity=1">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
            Download ZIP
          </a>
        </div>
        <div style="font-size:13px;color:var(--text-light);line-height:1.8;">
          <strong>What's new in v${LATEST_SCRAPER_VERSION}:</strong>
          <ul style="margin:8px 0 0 20px;padding:0;">
            <li>Wholesaler profiles captured for all deal posts — even when address can't be matched</li>
            <li>Improved poster name/profile detection with multiple fallback strategies</li>
            <li>Contact info (phone/email) extracted and linked to poster profiles automatically</li>
            <li>Poster matching by phone or email when name/URL not available</li>
            <li>Collector name required before scraping can start</li>
            <li>Version tracking — see who's up to date on the Sources page</li>
            <li>Broader Facebook post URL capture (fewer missing post links)</li>
          </ul>
        </div>
      </div>

      <div style="background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:24px;">
        <div style="font-size:16px;font-weight:700;margin-bottom:16px;">Installation Guide</div>
        <ol style="font-size:13px;color:var(--text-light);line-height:2.2;margin:0 0 0 20px;padding:0;">
          <li>Download the ZIP file above</li>
          <li>Unzip the file to a folder on your computer</li>
          <li>Open Chrome and go to <code style="background:var(--bg);padding:2px 6px;border-radius:4px;font-size:12px;">chrome://extensions</code></li>
          <li>Enable <strong>Developer mode</strong> (toggle in the top-right corner)</li>
          <li>Click <strong>"Load unpacked"</strong> and select the unzipped folder</li>
          <li>Click the extension icon and enter your <strong>Collector Name</strong></li>
          <li>Navigate to any Facebook group — scraping starts automatically!</li>
        </ol>
      </div>

      <div style="background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:24px;margin-top:24px;">
        <div style="font-size:16px;font-weight:700;margin-bottom:16px;">Updating an Existing Installation</div>
        <ol style="font-size:13px;color:var(--text-light);line-height:2.2;margin:0 0 0 20px;padding:0;">
          <li>Download the latest ZIP and unzip to a <strong>new folder</strong></li>
          <li>Go to <code style="background:var(--bg);padding:2px 6px;border-radius:4px;font-size:12px;">chrome://extensions</code></li>
          <li>Find "Off Market Radar" and click <strong>Remove</strong></li>
          <li>Click <strong>"Load unpacked"</strong> and select the new folder</li>
          <li>Your Collector Name and settings will be preserved</li>
        </ol>
      </div>
    </div>
  `;

  // Fetch the ZIP's last-modified timestamp
  fetch('fb-deal-scraper-latest.zip', { method: 'HEAD' })
    .then(function(resp) {
      const lm = resp.headers.get('last-modified');
      const el = document.getElementById('zipTimestamp');
      if (lm && el) {
        const d = new Date(lm);
        const mo = d.toLocaleString('en-US', { month: 'short' });
        const day = d.getDate();
        const yr = d.getFullYear();
        let hr = d.getHours();
        const mn = String(d.getMinutes()).padStart(2, '0');
        const ampm = hr >= 12 ? 'PM' : 'AM';
        hr = hr % 12 || 12;
        el.textContent = 'Built ' + mo + ' ' + day + ', ' + yr + ' at ' + hr + ':' + mn + ' ' + ampm;
      } else if (el) {
        el.textContent = '';
      }
    })
    .catch(function() {
      const el = document.getElementById('zipTimestamp');
      if (el) el.textContent = '';
    });
}

// ===== TEAM ACTIVITY =====

async function loadTeamActivity() {
  const container = document.getElementById('teamContent');
  if (!container) return;
  container.innerHTML = '<div class="loading"><div class="spinner"></div><div>Analyzing team activity...</div></div>';

  try {
    // Fetch all posts with collector_name, captured_at, match_status
    const allPosts = [];
    var tOffset = 0;
    var tBatchSize = 1000;
    var tKeepGoing = true;

    while (tKeepGoing) {
      var result = await supabaseGet('fb_deal_posts', {
        select: 'collector_name,captured_at,match_status,matched_address',
        order: 'captured_at.desc',
        limit: tBatchSize,
        offset: tOffset,
      });
      allPosts.push(...result.data);
      tOffset += tBatchSize;
      if (result.data.length < tBatchSize || allPosts.length >= result.count) tKeepGoing = false;
    }

    // Group posts by collector
    var byCollector = {};
    for (var p of allPosts) {
      var cn = p.collector_name || 'Unknown';
      if (!byCollector[cn]) byCollector[cn] = [];
      byCollector[cn].push(p);
    }

    // Build sessions per collector: a session = consecutive posts with <= 5 min gap
    var SESSION_GAP_MS = 5 * 60 * 1000; // 5 minutes
    var allSessions = [];

    for (var collector of Object.keys(byCollector)) {
      var posts = byCollector[collector]
        .map(function(p) { return { ts: new Date(p.captured_at), status: p.match_status, addr: p.matched_address }; })
        .sort(function(a, b) { return a.ts - b.ts; });

      var session = null;
      for (var i = 0; i < posts.length; i++) {
        if (!session) {
          session = { collector: collector, start: posts[i].ts, end: posts[i].ts, posts: [posts[i]] };
        } else {
          var gap = posts[i].ts - session.end;
          if (gap <= SESSION_GAP_MS) {
            session.end = posts[i].ts;
            session.posts.push(posts[i]);
          } else {
            allSessions.push(session);
            session = { collector: collector, start: posts[i].ts, end: posts[i].ts, posts: [posts[i]] };
          }
        }
      }
      if (session) allSessions.push(session);
    }

    // Calculate session stats
    var sessionRows = allSessions.map(function(s) {
      var durationMs = s.end - s.start;
      var durationMin = Math.round(durationMs / 60000);
      var matchedAddrs = new Set();
      for (var p of s.posts) {
        if ((p.status === 'matched' || p.status === 'multi_match' || p.status === 'confirmed') && p.addr) {
          matchedAddrs.add(p.addr);
        }
      }
      return {
        collector: s.collector,
        start: s.start,
        end: s.end,
        durationMin: durationMin,
        totalPosts: s.posts.length,
        uniqueMatches: matchedAddrs.size,
      };
    }).sort(function(a, b) { return b.start - a.start; });

    // Build daily summary per collector
    var dailyMap = {};
    for (var sess of sessionRows) {
      var dateKey = sess.start.toLocaleDateString('en-US', { timeZone: 'America/Chicago', year: 'numeric', month: '2-digit', day: '2-digit' });
      var dk = sess.collector + '|' + dateKey;
      if (!dailyMap[dk]) {
        dailyMap[dk] = { collector: sess.collector, date: dateKey, totalMinutes: 0, uniqueMatches: 0, totalPosts: 0, sessions: 0 };
      }
      dailyMap[dk].totalMinutes += sess.durationMin;
      dailyMap[dk].uniqueMatches += sess.uniqueMatches;
      dailyMap[dk].totalPosts += sess.totalPosts;
      dailyMap[dk].sessions++;
    }
    var dailyRows = Object.values(dailyMap).sort(function(a, b) {
      if (a.date !== b.date) return b.date.localeCompare(a.date);
      return a.collector.localeCompare(b.collector);
    });

    // Format duration
    function fmtDuration(mins) {
      if (mins < 1) return '<1m';
      var h = Math.floor(mins / 60);
      var m = mins % 60;
      if (h > 0) return h + 'h ' + m + 'm';
      return m + 'm';
    }

    function fmtHours(mins) {
      return (mins / 60).toFixed(1);
    }

    // KPIs
    var totalSessions = sessionRows.length;
    var totalHours = sessionRows.reduce(function(s, r) { return s + r.durationMin; }, 0) / 60;
    var totalUniqueMatches = sessionRows.reduce(function(s, r) { return s + r.uniqueMatches; }, 0);
    var activeCollectors = new Set(sessionRows.map(function(r) { return r.collector; })).size;

    // Render
    var sessionTableRows = sessionRows.slice(0, 100).map(function(r) {
      var mph = r.durationMin > 0 ? (r.uniqueMatches / (r.durationMin / 60)).toFixed(1) : '0';
      return '<tr>' +
        '<td style="font-weight:600;">' + escapeHtml(r.collector) + '</td>' +
        '<td>' + formatDate(r.start.toISOString()) + '</td>' +
        '<td>' + formatDate(r.end.toISOString()) + '</td>' +
        '<td style="font-weight:600;">' + fmtDuration(r.durationMin) + '</td>' +
        '<td>' + r.totalPosts + '</td>' +
        '<td style="color:var(--green);font-weight:600;">' + r.uniqueMatches + '</td>' +
        '<td style="color:var(--accent);font-weight:600;">' + mph + '</td>' +
      '</tr>';
    }).join('');

    var dailyTableRows = dailyRows.map(function(r) {
      var matchesPerHr = r.totalMinutes > 0 ? (r.uniqueMatches / (r.totalMinutes / 60)).toFixed(1) : '0';
      return '<tr>' +
        '<td>' + r.date + '</td>' +
        '<td style="font-weight:600;">' + escapeHtml(r.collector) + '</td>' +
        '<td>' + r.sessions + '</td>' +
        '<td style="font-weight:600;">' + fmtHours(r.totalMinutes) + 'h</td>' +
        '<td>' + r.totalPosts + '</td>' +
        '<td style="color:var(--green);font-weight:600;">' + r.uniqueMatches + '</td>' +
        '<td style="color:var(--accent);font-weight:600;">' + matchesPerHr + '</td>' +
      '</tr>';
    }).join('');

    // Build Summary tab: aggregate all users by day, then by week
    var summaryDailyMap = {};
    for (var sd of dailyRows) {
      if (!summaryDailyMap[sd.date]) {
        summaryDailyMap[sd.date] = { date: sd.date, collectors: 0, sessions: 0, totalMinutes: 0, totalPosts: 0, uniqueMatches: 0 };
      }
      summaryDailyMap[sd.date].collectors++;
      summaryDailyMap[sd.date].sessions += sd.sessions;
      summaryDailyMap[sd.date].totalMinutes += sd.totalMinutes;
      summaryDailyMap[sd.date].totalPosts += sd.totalPosts;
      summaryDailyMap[sd.date].uniqueMatches += sd.uniqueMatches;
    }
    var summaryDailyArr = Object.values(summaryDailyMap).sort(function(a, b) { return b.date.localeCompare(a.date); });

    // Weekly aggregation — group by week starting Monday
    function getWeekKey(dateStr) {
      // dateStr is MM/DD/YYYY
      var parts = dateStr.split('/');
      var d = new Date(parts[2], parts[0] - 1, parts[1]);
      var day = d.getDay();
      var diff = d.getDate() - day + (day === 0 ? -6 : 1); // Monday
      var monday = new Date(d.setDate(diff));
      var sun = new Date(monday);
      sun.setDate(sun.getDate() + 6);
      var fmt = function(dt) { return (dt.getMonth()+1) + '/' + dt.getDate() + '/' + String(dt.getFullYear()).slice(2); };
      return fmt(monday) + ' – ' + fmt(sun);
    }
    var summaryWeeklyMap = {};
    for (var sw of summaryDailyArr) {
      var wk = getWeekKey(sw.date);
      if (!summaryWeeklyMap[wk]) {
        summaryWeeklyMap[wk] = { week: wk, days: 0, collectors: new Set(), sessions: 0, totalMinutes: 0, totalPosts: 0, uniqueMatches: 0 };
      }
      summaryWeeklyMap[wk].days++;
      // Can't easily track unique collectors across days in simple aggregation, use max
      summaryWeeklyMap[wk].collectors.add(sw.date); // placeholder — we'll fix below
      summaryWeeklyMap[wk].sessions += sw.sessions;
      summaryWeeklyMap[wk].totalMinutes += sw.totalMinutes;
      summaryWeeklyMap[wk].totalPosts += sw.totalPosts;
      summaryWeeklyMap[wk].uniqueMatches += sw.uniqueMatches;
    }
    // Fix collector counts per week
    var weeklyCollectorMap = {};
    for (var wcd of dailyRows) {
      var wk2 = getWeekKey(wcd.date);
      if (!weeklyCollectorMap[wk2]) weeklyCollectorMap[wk2] = new Set();
      weeklyCollectorMap[wk2].add(wcd.collector);
    }
    var summaryWeeklyArr = Object.values(summaryWeeklyMap).map(function(w) {
      var ck = weeklyCollectorMap[w.week];
      return { week: w.week, days: w.days, collectors: ck ? ck.size : 0, sessions: w.sessions, totalMinutes: w.totalMinutes, totalPosts: w.totalPosts, uniqueMatches: w.uniqueMatches };
    }).sort(function(a, b) { return b.week.localeCompare(a.week); });

    var summaryDailyTableRows = summaryDailyArr.map(function(r) {
      var mph = r.totalMinutes > 0 ? (r.uniqueMatches / (r.totalMinutes / 60)).toFixed(1) : '0';
      return '<tr>' +
        '<td style="font-weight:600;">' + r.date + '</td>' +
        '<td>' + r.collectors + '</td>' +
        '<td>' + r.sessions + '</td>' +
        '<td style="font-weight:600;">' + fmtHours(r.totalMinutes) + 'h</td>' +
        '<td>' + r.totalPosts.toLocaleString() + '</td>' +
        '<td style="color:var(--green);font-weight:600;">' + r.uniqueMatches + '</td>' +
        '<td style="color:var(--accent);font-weight:600;">' + mph + '</td>' +
      '</tr>';
    }).join('');

    var summaryWeeklyTableRows = summaryWeeklyArr.map(function(r) {
      var mph = r.totalMinutes > 0 ? (r.uniqueMatches / (r.totalMinutes / 60)).toFixed(1) : '0';
      return '<tr>' +
        '<td style="font-weight:600;">' + r.week + '</td>' +
        '<td>' + r.days + '</td>' +
        '<td>' + r.collectors + '</td>' +
        '<td>' + r.sessions + '</td>' +
        '<td style="font-weight:600;">' + fmtHours(r.totalMinutes) + 'h</td>' +
        '<td>' + r.totalPosts.toLocaleString() + '</td>' +
        '<td style="color:var(--green);font-weight:600;">' + r.uniqueMatches + '</td>' +
        '<td style="color:var(--accent);font-weight:600;">' + mph + '</td>' +
      '</tr>';
    }).join('');

    var activeTeamTab = window._teamTab || 'summary';

    container.innerHTML = '<div class="kpi-grid" style="grid-template-columns:repeat(4,1fr);margin-bottom:20px;">' +
      '<div class="kpi-card blue"><div class="kpi-glow"></div><div class="kpi-value">' + activeCollectors + '</div><div class="kpi-label">Active Collectors</div></div>' +
      '<div class="kpi-card purple"><div class="kpi-glow"></div><div class="kpi-value">' + totalSessions + '</div><div class="kpi-label">Total Sessions</div></div>' +
      '<div class="kpi-card cyan"><div class="kpi-glow"></div><div class="kpi-value">' + totalHours.toFixed(1) + 'h</div><div class="kpi-label">Total Hours</div></div>' +
      '<div class="kpi-card green"><div class="kpi-glow"></div><div class="kpi-value">' + totalUniqueMatches.toLocaleString() + '</div><div class="kpi-label">Unique Matches</div></div>' +
    '</div>' +

    '<div style="display:flex;gap:0;margin-bottom:16px;border-bottom:2px solid var(--border);">' +
      '<button onclick="switchTeamTab(\'summary\')" id="teamTabSummary" style="padding:10px 24px;font-size:14px;font-weight:600;cursor:pointer;border:none;background:none;color:' + (activeTeamTab === 'summary' ? 'var(--green)' : 'var(--text-muted)') + ';border-bottom:2px solid ' + (activeTeamTab === 'summary' ? 'var(--green)' : 'transparent') + ';margin-bottom:-2px;transition:all 0.15s;">' +
        '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px;margin-right:6px;"><path d="M12 20V10"/><path d="M18 20V4"/><path d="M6 20v-4"/></svg>' +
        'Summary' +
      '</button>' +
      '<button onclick="switchTeamTab(\'sessions\')" id="teamTabSessions" style="padding:10px 24px;font-size:14px;font-weight:600;cursor:pointer;border:none;background:none;color:' + (activeTeamTab === 'sessions' ? 'var(--accent)' : 'var(--text-muted)') + ';border-bottom:2px solid ' + (activeTeamTab === 'sessions' ? 'var(--accent)' : 'transparent') + ';margin-bottom:-2px;transition:all 0.15s;">' +
        '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px;margin-right:6px;"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>' +
        'Session History (' + sessionRows.length + ')' +
      '</button>' +
      '<button onclick="switchTeamTab(\'userdaily\')" id="teamTabUserdaily" style="padding:10px 24px;font-size:14px;font-weight:600;cursor:pointer;border:none;background:none;color:' + (activeTeamTab === 'userdaily' ? 'var(--purple)' : 'var(--text-muted)') + ';border-bottom:2px solid ' + (activeTeamTab === 'userdaily' ? 'var(--purple)' : 'transparent') + ';margin-bottom:-2px;transition:all 0.15s;">' +
        '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px;margin-right:6px;"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>' +
        'User Daily Summary (' + dailyRows.length + ')' +
      '</button>' +
    '</div>' +

    '<div id="teamSummaryPanel" style="' + (activeTeamTab !== 'summary' ? 'display:none;' : '') + '">' +
      '<div style="font-size:15px;font-weight:700;margin-bottom:10px;">By Day</div>' +
      '<table class="deals-table" style="margin-bottom:32px;">' +
        '<thead><tr>' +
          '<th>Date</th>' +
          '<th>Collectors</th>' +
          '<th>Sessions</th>' +
          '<th>Total Hours</th>' +
          '<th>Posts Scanned</th>' +
          '<th>Unique Matches</th>' +
          '<th>Matches/Hour</th>' +
        '</tr></thead>' +
        '<tbody>' + (summaryDailyTableRows || '<tr><td colspan="7" style="text-align:center;color:var(--text-muted);padding:40px;">No data</td></tr>') + '</tbody>' +
      '</table>' +
      '<div style="font-size:15px;font-weight:700;margin-bottom:10px;">By Week</div>' +
      '<table class="deals-table">' +
        '<thead><tr>' +
          '<th>Week</th>' +
          '<th>Days Active</th>' +
          '<th>Collectors</th>' +
          '<th>Sessions</th>' +
          '<th>Total Hours</th>' +
          '<th>Posts Scanned</th>' +
          '<th>Unique Matches</th>' +
          '<th>Matches/Hour</th>' +
        '</tr></thead>' +
        '<tbody>' + (summaryWeeklyTableRows || '<tr><td colspan="8" style="text-align:center;color:var(--text-muted);padding:40px;">No data</td></tr>') + '</tbody>' +
      '</table>' +
    '</div>' +

    '<div id="teamSessionsPanel" style="' + (activeTeamTab !== 'sessions' ? 'display:none;' : '') + '">' +
      '<div style="font-size:12px;color:var(--text-muted);margin-bottom:12px;">A session is a block of scanning where no more than 5 minutes elapses between posts.</div>' +
      '<table class="deals-table">' +
        '<thead><tr>' +
          '<th>Collector</th>' +
          '<th>Start Time</th>' +
          '<th>End Time</th>' +
          '<th>Duration</th>' +
          '<th>Posts Scanned</th>' +
          '<th>Unique Matches</th>' +
          '<th>Matches/Hour</th>' +
        '</tr></thead>' +
        '<tbody>' + (sessionTableRows || '<tr><td colspan="7" style="text-align:center;color:var(--text-muted);padding:40px;">No sessions found</td></tr>') + '</tbody>' +
      '</table>' +
    '</div>' +

    '<div id="teamUserdailyPanel" style="' + (activeTeamTab !== 'userdaily' ? 'display:none;' : '') + '">' +
      '<table class="deals-table">' +
        '<thead><tr>' +
          '<th>Date</th>' +
          '<th>Collector</th>' +
          '<th>Sessions</th>' +
          '<th>Total Hours</th>' +
          '<th>Posts Scanned</th>' +
          '<th>Unique Matches</th>' +
          '<th>Matches/Hour</th>' +
        '</tr></thead>' +
        '<tbody>' + (dailyTableRows || '<tr><td colspan="7" style="text-align:center;color:var(--text-muted);padding:40px;">No data</td></tr>') + '</tbody>' +
      '</table>' +
    '</div>';

  } catch (err) {
    console.error('Failed to load team activity:', err);
    container.innerHTML = '<div style="color:var(--red);text-align:center;padding:40px;">Failed to load team activity: ' + escapeHtml(err.message) + '</div>';
  }
}

function switchTeamTab(tab) {
  window._teamTab = tab;
  var panels = ['Summary', 'Sessions', 'Userdaily'];
  var colors = { summary: 'var(--green)', sessions: 'var(--accent)', userdaily: 'var(--purple)' };
  for (var i = 0; i < panels.length; i++) {
    var key = panels[i].toLowerCase();
    var panel = document.getElementById('team' + panels[i] + 'Panel');
    var btn = document.getElementById('teamTab' + panels[i]);
    if (panel) panel.style.display = (key === tab) ? '' : 'none';
    if (btn) {
      btn.style.color = (key === tab) ? colors[key] : 'var(--text-muted)';
      btn.style.borderBottomColor = (key === tab) ? colors[key] : 'transparent';
    }
  }
}

// ===== OUTREACH =====
let outreachTab = 'comments'; // 'comments', 'dms', or 'other'
let outreachHideDone = false;

// Track outreach status in localStorage
function getOutreachDone() {
  try { return JSON.parse(localStorage.getItem('omr_outreach_done') || '{}'); } catch(e) { return {}; }
}
function markOutreachDone(postId) {
  var done = getOutreachDone();
  done[postId] = new Date().toISOString();
  localStorage.setItem('omr_outreach_done', JSON.stringify(done));
  // Also persist to Supabase
  supabasePatch('fb_deal_posts', postId, { outreach_status: 'contacted' }).catch(function(e) {
    console.warn('Failed to save outreach status to DB:', e);
  });
}

// Patterns to classify outreach type
const COMMENT_PATTERN = /\bcomment\b|\bleave.*(email|info|number|details)\b|\bdrop.*(email|info|number|details)\b|\bsend.*(email|info|number)\b|\bput.*(email|info)\b|\bemail\s*(below|me|in)\b|\bcomment\s*(below|your|with)\b/i;
const DM_PATTERN = /\bDM\b|\bdirect\s*message\b|\binbox\s*me\b|\bPM\b|\bprivate\s*message\b|\bmessage\s*me\b|\bsend\s*me\s*a\s*message\b|\bshoot\s*me\s*a\s*(dm|message|pm)\b|\bhit\s*me\s*up\b/i;

// ===== TRANSACTIONS VIEW =====
async function loadTransactions() {
  const container = document.getElementById('transactionsContent');
  if (!container) return;

  container.innerHTML = '<div style="text-align:center;padding:60px;color:var(--text-muted);">Loading transactions...</div>';

  try {
    // Fetch all matched/confirmed deals with transaction data
    const { data: allDeals } = await supabaseGetAll('fb_deal_posts', {
      select: 'id,poster_name,captured_at,post_timestamp,matched_address,parsed_asking_price,parsed_arv,transaction_status,transaction_sold_date,transaction_sold_price,transaction_sale_source,match_status,match_candidates',
      filters: [{ col: 'match_status', val: 'in.(matched,confirmed)' }],
    });

    // Separate sold vs all matched
    const soldDeals = allDeals.filter(d => d.transaction_status === 'sold');
    const checkedDeals = allDeals.filter(d => d.transaction_status && d.transaction_status !== 'null');
    const uncheckedDeals = allDeals.filter(d => !d.transaction_status);

    // Compute wholesaler transaction rates
    const wholesalerMap = {};
    for (const d of allDeals) {
      const name = d.poster_name || 'Unknown';
      if (!wholesalerMap[name]) wholesalerMap[name] = { total: 0, sold: 0, totalAsk: 0, totalSold: 0 };
      wholesalerMap[name].total++;
      if (d.transaction_status === 'sold') {
        wholesalerMap[name].sold++;
        wholesalerMap[name].totalSold += Number(d.transaction_sold_price) || 0;
      }
      wholesalerMap[name].totalAsk += Number(d.parsed_asking_price) || Number(d.parsed_arv) || 0;
    }

    const wholesalerRates = Object.entries(wholesalerMap)
      .map(([name, s]) => ({ name, ...s, rate: s.total > 0 ? (s.sold / s.total * 100) : 0 }))
      .sort((a, b) => b.sold - a.sold || b.total - a.total);

    // Total GMV from sold deals
    const soldGMV = soldDeals.reduce((sum, d) => sum + (Number(d.transaction_sold_price) || 0), 0);
    const avgSoldPrice = soldDeals.length > 0 ? soldGMV / soldDeals.length : 0;
    const overallRate = allDeals.length > 0 ? (soldDeals.length / allDeals.length * 100) : 0;

    const fmtMoney = (v) => v >= 1000000 ? '$' + (v/1000000).toFixed(1) + 'M' : v >= 1000 ? '$' + (v/1000).toFixed(0) + 'K' : '$' + v.toFixed(0);
    const fmtDate = (d) => {
      if (!d) return '—';
      const dt = new Date(d);
      return (dt.getMonth()+1) + '/' + dt.getDate() + '/' + dt.getFullYear();
    };

    // KPI cards
    let html = '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:16px;margin-bottom:24px;">';
    html += '<div class="card" style="padding:18px;text-align:center;">';
    html += '<div style="font-size:28px;font-weight:700;color:var(--green);">' + soldDeals.length + '</div>';
    html += '<div style="font-size:12px;color:var(--text-muted);margin-top:4px;">Confirmed Sales</div></div>';

    html += '<div class="card" style="padding:18px;text-align:center;">';
    html += '<div style="font-size:28px;font-weight:700;color:var(--accent);">' + fmtMoney(soldGMV) + '</div>';
    html += '<div style="font-size:12px;color:var(--text-muted);margin-top:4px;">Sold Volume</div></div>';

    html += '<div class="card" style="padding:18px;text-align:center;">';
    html += '<div style="font-size:28px;font-weight:700;color:var(--purple);">' + overallRate.toFixed(1) + '%</div>';
    html += '<div style="font-size:12px;color:var(--text-muted);margin-top:4px;">Transaction Rate</div></div>';

    html += '<div class="card" style="padding:18px;text-align:center;">';
    html += '<div style="font-size:28px;font-weight:700;color:var(--text-light);">' + fmtMoney(avgSoldPrice) + '</div>';
    html += '<div style="font-size:12px;color:var(--text-muted);margin-top:4px;">Avg Sold Price</div></div>';

    html += '<div class="card" style="padding:18px;text-align:center;">';
    html += '<div style="font-size:28px;font-weight:700;color:var(--text-light);">' + checkedDeals.length + ' / ' + allDeals.length + '</div>';
    html += '<div style="font-size:12px;color:var(--text-muted);margin-top:4px;">Checked / Total Matched</div></div>';
    html += '</div>';

    // Tab bar
    html += '<div style="display:flex;gap:0;margin-bottom:20px;border-bottom:2px solid var(--border);">';
    html += '<button class="tx-tab active" data-tab="details" style="padding:10px 24px;border:none;background:none;color:var(--accent);font-weight:600;font-size:13px;cursor:pointer;border-bottom:2px solid var(--accent);margin-bottom:-2px;">Transaction Details</button>';
    html += '<button class="tx-tab" data-tab="wholesalers" style="padding:10px 24px;border:none;background:none;color:var(--text-muted);font-weight:600;font-size:13px;cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-2px;">Wholesaler Rates</button>';
    html += '</div>';

    // ─── Tab: Transaction Details ───
    html += '<div id="tx-tab-details" class="tx-tab-content">';
    html += '<div class="card" style="padding:20px;">';
    html += '<div style="font-size:14px;font-weight:600;margin-bottom:14px;color:var(--text-light);">Sold Properties (' + soldDeals.length + ')</div>';

    if (soldDeals.length === 0) {
      html += '<div style="text-align:center;padding:40px;color:var(--text-muted);">';
      if (uncheckedDeals.length > 0) {
        html += 'Transaction check is running — ' + uncheckedDeals.length + ' deals still being analyzed.<br>Check back soon.';
      } else {
        html += 'No confirmed sales found yet within the 6-week window.';
      }
      html += '</div>';
    } else {
      soldDeals.sort((a, b) => new Date(b.transaction_sold_date) - new Date(a.transaction_sold_date));

      html += '<div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;font-size:13px;">';
      html += '<thead><tr style="border-bottom:1px solid var(--border);color:var(--text-muted);font-size:11px;text-transform:uppercase;letter-spacing:0.5px;">';
      html += '<th style="text-align:left;padding:8px 12px;">Wholesaler</th>';
      html += '<th style="text-align:left;padding:8px 12px;">Post Date</th>';
      html += '<th style="text-align:left;padding:8px 12px;">Address</th>';
      html += '<th style="text-align:right;padding:8px 12px;">Ask Price</th>';
      html += '<th style="text-align:left;padding:8px 12px;">Sold Date</th>';
      html += '<th style="text-align:right;padding:8px 12px;">Sold Price</th>';
      html += '<th style="text-align:center;padding:8px 12px;">Days to Close</th>';
      html += '</tr></thead><tbody>';

      for (const d of soldDeals) {
        const postDate = fmtDate(d.post_timestamp || d.captured_at);
        const soldDate = fmtDate(d.transaction_sold_date);
        const askPrice = Number(d.parsed_asking_price) || Number(d.parsed_arv) || 0;
        const soldPrice = Number(d.transaction_sold_price) || 0;
        const priceDiff = soldPrice > 0 && askPrice > 0 ? ((soldPrice - askPrice) / askPrice * 100) : null;
        const diffColor = priceDiff !== null ? (priceDiff >= 0 ? 'var(--green)' : 'var(--red, #ef5350)') : '';
        // Days between post and sale
        const postDt = new Date(d.post_timestamp || d.captured_at);
        const soldDt = new Date(d.transaction_sold_date);
        const daysToClose = !isNaN(postDt) && !isNaN(soldDt) ? Math.round((soldDt - postDt) / 86400000) : null;

        html += '<tr style="border-bottom:1px solid var(--border);">';
        html += '<td style="padding:10px 12px;font-weight:500;">' + escapeHtml(d.poster_name || 'Unknown') + '</td>';
        html += '<td style="padding:10px 12px;color:var(--text-muted);">' + postDate + '</td>';
        html += '<td style="padding:10px 12px;">' + escapeHtml(d.matched_address || '—') + '</td>';
        html += '<td style="padding:10px 12px;text-align:right;">' + (askPrice > 0 ? fmtMoney(askPrice) : '—') + '</td>';
        html += '<td style="padding:10px 12px;color:var(--green);font-weight:500;">' + soldDate + '</td>';
        html += '<td style="padding:10px 12px;text-align:right;font-weight:600;">' + (soldPrice > 0 ? fmtMoney(soldPrice) : '—');
        if (priceDiff !== null) {
          html += ' <span style="font-size:11px;color:' + diffColor + ';">(' + (priceDiff >= 0 ? '+' : '') + priceDiff.toFixed(0) + '%)</span>';
        }
        html += '</td>';
        html += '<td style="padding:10px 12px;text-align:center;color:var(--text-muted);">' + (daysToClose !== null ? daysToClose + 'd' : '—') + '</td>';
        html += '</tr>';
      }
      html += '</tbody></table></div>';
    }
    html += '</div></div>';

    // ─── Tab: Wholesaler Rates ───
    html += '<div id="tx-tab-wholesalers" class="tx-tab-content" style="display:none;">';
    html += '<div class="card" style="padding:20px;">';
    html += '<div style="font-size:14px;font-weight:600;margin-bottom:14px;color:var(--text-light);">Wholesaler Transaction Rates</div>';
    html += '<div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;font-size:13px;">';
    html += '<thead><tr style="border-bottom:1px solid var(--border);color:var(--text-muted);font-size:11px;text-transform:uppercase;letter-spacing:0.5px;">';
    html += '<th style="text-align:left;padding:8px 12px;">Wholesaler</th>';
    html += '<th style="text-align:center;padding:8px 12px;">Matched Deals</th>';
    html += '<th style="text-align:center;padding:8px 12px;">Sold</th>';
    html += '<th style="text-align:center;padding:8px 12px;">Rate</th>';
    html += '<th style="text-align:right;padding:8px 12px;">Total Sold Vol</th>';
    html += '</tr></thead><tbody>';

    for (const w of wholesalerRates.slice(0, 30)) {
      const rateColor = w.rate >= 20 ? 'var(--green)' : w.rate >= 10 ? 'var(--accent)' : 'var(--text-muted)';
      html += '<tr style="border-bottom:1px solid var(--border);">';
      html += '<td style="padding:10px 12px;font-weight:500;">' + escapeHtml(w.name) + '</td>';
      html += '<td style="padding:10px 12px;text-align:center;">' + w.total + '</td>';
      html += '<td style="padding:10px 12px;text-align:center;font-weight:600;color:var(--green);">' + w.sold + '</td>';
      html += '<td style="padding:10px 12px;text-align:center;font-weight:600;color:' + rateColor + ';">' + w.rate.toFixed(1) + '%</td>';
      html += '<td style="padding:10px 12px;text-align:right;">' + (w.totalSold > 0 ? fmtMoney(w.totalSold) : '—') + '</td>';
      html += '</tr>';
    }
    html += '</tbody></table></div></div></div>';

    container.innerHTML = html;

    // Enforce initial tab visibility (details shown, wholesalers hidden)
    var detailsTab = document.getElementById('tx-tab-details');
    var wholesalersTab = document.getElementById('tx-tab-wholesalers');
    if (detailsTab) detailsTab.style.display = 'block';
    if (wholesalersTab) wholesalersTab.style.display = 'none';

    // Tab switching logic
    container.querySelectorAll('.tx-tab').forEach(function(btn) {
      btn.addEventListener('click', function() {
        container.querySelectorAll('.tx-tab').forEach(function(b) {
          b.classList.remove('active');
          b.style.color = 'var(--text-muted)';
          b.style.borderBottomColor = 'transparent';
        });
        btn.classList.add('active');
        btn.style.color = 'var(--accent)';
        btn.style.borderBottomColor = 'var(--accent)';
        container.querySelectorAll('.tx-tab-content').forEach(function(c) { c.style.display = 'none'; });
        var tabId = 'tx-tab-' + btn.getAttribute('data-tab');
        var tabEl = document.getElementById(tabId);
        if (tabEl) tabEl.style.display = 'block';
      });
    });
  } catch (err) {
    console.error('Failed to load transactions:', err);
    container.innerHTML = '<div style="text-align:center;padding:60px;color:var(--text-muted);">Error loading transactions: ' + escapeHtml(err.message) + '</div>';
  }
}

async function loadQualityControl() {
  const container = document.getElementById('qcContent');
  if (!container) return;
  container.innerHTML = '<div style="text-align:center;padding:60px;color:var(--text-muted);">Loading quality control data...</div>';

  try {
    // Fetch all matched/multi_match/confirmed deals with parsed fields and candidates
    const { data: deals } = await supabaseGetAll('fb_deal_posts', {
      select: 'id,parsed_full_address,parsed_city,parsed_state,parsed_zip,parsed_beds,parsed_baths,parsed_sqft,parsed_year_built,parsed_lot_sqft,matched_address,match_status,match_confidence,match_candidates,poster_name,captured_at',
      filters: [{ col: 'match_status', val: 'in.(matched,multi_match,confirmed)' }],
    });

    // Sort by capture date descending
    deals.sort((a, b) => new Date(b.captured_at) - new Date(a.captured_at));

    // KPI cards
    const totalMatched = deals.length;
    const withAttom = deals.filter(d => {
      const c = d.match_candidates?.[0];
      return c && c.attom_id;
    }).length;
    const noAttom = totalMatched - withAttom;
    const exactCount = deals.filter(d => d.match_confidence === 'exact').length;
    const multiCount = deals.filter(d => d.match_status === 'multi_match').length;

    let html = '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:16px;margin-bottom:24px;">';
    html += '<div class="card" style="padding:18px;text-align:center;">';
    html += '<div style="font-size:28px;font-weight:700;color:var(--accent);">' + totalMatched + '</div>';
    html += '<div style="font-size:12px;color:var(--text-muted);margin-top:4px;">Total Matched</div></div>';
    html += '<div class="card" style="padding:18px;text-align:center;">';
    html += '<div style="font-size:28px;font-weight:700;color:var(--green);">' + withAttom + '</div>';
    html += '<div style="font-size:12px;color:var(--text-muted);margin-top:4px;">With ATTOM ID</div></div>';
    html += '<div class="card" style="padding:18px;text-align:center;">';
    html += '<div style="font-size:28px;font-weight:700;color:var(--red, #ef5350);">' + noAttom + '</div>';
    html += '<div style="font-size:12px;color:var(--text-muted);margin-top:4px;">Missing ATTOM ID</div></div>';
    html += '<div class="card" style="padding:18px;text-align:center;">';
    html += '<div style="font-size:28px;font-weight:700;color:var(--purple);">' + exactCount + '</div>';
    html += '<div style="font-size:12px;color:var(--text-muted);margin-top:4px;">Exact Matches</div></div>';
    html += '<div class="card" style="padding:18px;text-align:center;">';
    html += '<div style="font-size:28px;font-weight:700;color:var(--text-muted);">' + multiCount + '</div>';
    html += '<div style="font-size:12px;color:var(--text-muted);margin-top:4px;">Multi-Match</div></div>';
    html += '</div>';

    // Filter controls
    html += '<div class="card" style="padding:16px;margin-bottom:20px;display:flex;gap:12px;align-items:center;flex-wrap:wrap;">';
    html += '<label style="font-size:12px;color:var(--text-muted);font-weight:600;">FILTER:</label>';
    html += '<button class="qc-filter" data-filter="all" style="padding:6px 14px;border:1px solid var(--border);border-radius:6px;background:var(--surface);color:var(--text-muted);font-size:12px;cursor:pointer;">All (' + totalMatched + ')</button>';
    html += '<button class="qc-filter" data-filter="no-attom" style="padding:6px 14px;border:1px solid var(--border);border-radius:6px;background:var(--surface);color:var(--text-muted);font-size:12px;cursor:pointer;">Missing ATTOM (' + noAttom + ')</button>';
    html += '<button class="qc-filter active" data-filter="exact" style="padding:6px 14px;border:1px solid var(--border);border-radius:6px;background:var(--accent);color:#fff;font-size:12px;cursor:pointer;">Exact (' + exactCount + ')</button>';
    html += '<button class="qc-filter" data-filter="multi" style="padding:6px 14px;border:1px solid var(--border);border-radius:6px;background:var(--surface);color:var(--text-muted);font-size:12px;cursor:pointer;">Multi-Match (' + multiCount + ')</button>';
    html += '</div>';

    // Table
    html += '<div class="card" style="padding:20px;">';
    html += '<div style="overflow-x:auto;"><table id="qcTable" style="width:100%;border-collapse:collapse;font-size:12px;">';
    html += '<thead><tr style="border-bottom:2px solid var(--border);color:var(--text-muted);font-size:10px;text-transform:uppercase;letter-spacing:0.5px;">';
    html += '<th style="text-align:left;padding:8px 6px;">Confidence</th>';
    html += '<th colspan="10" style="text-align:center;padding:8px 6px;border-left:2px solid var(--border);background:rgba(99,102,241,0.05);">PARSED FROM POST</th>';
    html += '<th colspan="10" style="text-align:center;padding:8px 6px;border-left:2px solid var(--border);background:rgba(34,197,94,0.05);">DATABASE MATCH</th>';
    html += '<th style="text-align:left;padding:8px 6px;border-left:2px solid var(--border);">ATTOM ID</th>';
    html += '</tr>';
    html += '<tr style="border-bottom:1px solid var(--border);color:var(--text-muted);font-size:10px;text-transform:uppercase;letter-spacing:0.5px;">';
    html += '<th style="text-align:left;padding:6px;">Type</th>';
    // Parsed columns
    html += '<th style="text-align:left;padding:6px;border-left:2px solid var(--border);">Street #</th>';
    html += '<th style="text-align:left;padding:6px;">Street Name</th>';
    html += '<th style="text-align:left;padding:6px;">City</th>';
    html += '<th style="text-align:left;padding:6px;">St</th>';
    html += '<th style="text-align:left;padding:6px;">Zip</th>';
    html += '<th style="text-align:center;padding:6px;">Beds</th>';
    html += '<th style="text-align:center;padding:6px;">Baths</th>';
    html += '<th style="text-align:center;padding:6px;">SqFt</th>';
    html += '<th style="text-align:center;padding:6px;">Yr Built</th>';
    html += '<th style="text-align:center;padding:6px;">Lot</th>';
    // DB columns
    html += '<th style="text-align:left;padding:6px;border-left:2px solid var(--border);">Street #</th>';
    html += '<th style="text-align:left;padding:6px;">Street Name</th>';
    html += '<th style="text-align:left;padding:6px;">City</th>';
    html += '<th style="text-align:left;padding:6px;">St</th>';
    html += '<th style="text-align:left;padding:6px;">Zip</th>';
    html += '<th style="text-align:center;padding:6px;">Beds</th>';
    html += '<th style="text-align:center;padding:6px;">Baths</th>';
    html += '<th style="text-align:center;padding:6px;">SqFt</th>';
    html += '<th style="text-align:center;padding:6px;">Yr Built</th>';
    html += '<th style="text-align:center;padding:6px;">Lot</th>';
    html += '<th style="text-align:left;padding:6px;border-left:2px solid var(--border);">ID</th>';
    html += '</tr></thead><tbody>';

    for (const d of deals) {
      // Parse the full_address to extract street number and name
      const parsedAddr = d.parsed_full_address || '';
      const parsedMatch = parsedAddr.match(/^(\d+[A-Za-z]?)\s+(.+)/);
      const parsedStreetNum = parsedMatch ? parsedMatch[1] : '';
      const parsedStreetName = parsedMatch ? parsedMatch[2] : parsedAddr;

      // DB match data from first candidate
      const c = d.match_candidates?.[0] || {};
      const dbStreetNum = c.property_address_house_number || '';
      const dbStreetName = c.property_address_street_name || '';
      const dbCity = c.property_address_city || '';
      const dbState = c.property_address_state || '';
      const dbZip = c.property_address_zip || '';
      const attomId = c.attom_id || '';
      const hasAttom = !!attomId;

      // Confidence badge color
      const confColor = d.match_confidence === 'exact' ? 'var(--green)' :
                         d.match_confidence === 'high' ? 'var(--accent)' :
                         d.match_confidence === 'medium' ? 'orange' : 'var(--text-muted)';
      const statusLabel = d.match_status === 'multi_match' ? 'multi' : d.match_confidence || '—';

      // Data attributes for filtering
      const filterAttrs = 'data-has-attom="' + (hasAttom ? 'yes' : 'no') + '" data-confidence="' + (d.match_confidence || '') + '" data-status="' + d.match_status + '"';

      html += '<tr class="qc-row" ' + filterAttrs + ' style="border-bottom:1px solid var(--border);">';
      html += '<td style="padding:8px 6px;"><span style="display:inline-block;padding:2px 8px;border-radius:4px;font-size:10px;font-weight:600;background:' + confColor + '22;color:' + confColor + ';">' + escapeHtml(statusLabel) + '</span></td>';

      // Parsed columns
      const parsedBeds = d.parsed_beds || '';
      const parsedBaths = d.parsed_baths || '';
      const parsedSqft = d.parsed_sqft || '';
      const parsedYearBuilt = d.parsed_year_built || '';
      const parsedLotSqft = d.parsed_lot_sqft || '';
      const parsedLot = parsedLotSqft ? (Math.round(parsedLotSqft / 43560 * 100) / 100) + 'ac' : '';

      html += '<td style="padding:8px 6px;border-left:2px solid var(--border);font-weight:500;">' + escapeHtml(parsedStreetNum) + '</td>';
      html += '<td style="padding:8px 6px;">' + escapeHtml(parsedStreetName) + '</td>';
      html += '<td style="padding:8px 6px;">' + escapeHtml(d.parsed_city || '') + '</td>';
      html += '<td style="padding:8px 6px;">' + escapeHtml(d.parsed_state || '') + '</td>';
      html += '<td style="padding:8px 6px;">' + escapeHtml(d.parsed_zip || '') + '</td>';
      html += '<td style="padding:8px 6px;text-align:center;">' + escapeHtml(String(parsedBeds || '—')) + '</td>';
      html += '<td style="padding:8px 6px;text-align:center;">' + escapeHtml(String(parsedBaths || '—')) + '</td>';
      html += '<td style="padding:8px 6px;text-align:center;">' + (parsedSqft ? escapeHtml(Number(parsedSqft).toLocaleString()) : '—') + '</td>';
      html += '<td style="padding:8px 6px;text-align:center;">' + escapeHtml(String(parsedYearBuilt || '—')) + '</td>';
      html += '<td style="padding:8px 6px;text-align:center;">' + escapeHtml(parsedLot || '—') + '</td>';

      // DB match columns
      const dbBeds = c.bedrooms_count || '';
      const dbBaths = c.bath_count || '';
      const dbSqft = c.area_building || c.living_area_size || '';
      const dbYearBuilt = c.year_built || '';
      const dbLotSf = c.area_lot_sf || '';
      const dbLot = dbLotSf ? (Math.round(dbLotSf / 43560 * 100) / 100) + 'ac' : '';

      html += '<td style="padding:8px 6px;border-left:2px solid var(--border);font-weight:500;color:var(--green);">' + escapeHtml(dbStreetNum) + '</td>';
      html += '<td style="padding:8px 6px;color:var(--green);">' + escapeHtml(dbStreetName) + '</td>';
      html += '<td style="padding:8px 6px;color:var(--green);">' + escapeHtml(dbCity) + '</td>';
      html += '<td style="padding:8px 6px;color:var(--green);">' + escapeHtml(dbState) + '</td>';
      html += '<td style="padding:8px 6px;color:var(--green);">' + escapeHtml(dbZip) + '</td>';
      html += '<td style="padding:8px 6px;text-align:center;color:var(--green);">' + escapeHtml(String(dbBeds || '—')) + '</td>';
      html += '<td style="padding:8px 6px;text-align:center;color:var(--green);">' + escapeHtml(String(dbBaths || '—')) + '</td>';
      html += '<td style="padding:8px 6px;text-align:center;color:var(--green);">' + (dbSqft ? escapeHtml(Number(dbSqft).toLocaleString()) : '—') + '</td>';
      html += '<td style="padding:8px 6px;text-align:center;color:var(--green);">' + escapeHtml(String(dbYearBuilt || '—')) + '</td>';
      html += '<td style="padding:8px 6px;text-align:center;color:var(--green);">' + escapeHtml(dbLot || '—') + '</td>';

      // ATTOM ID
      html += '<td style="padding:8px 6px;border-left:2px solid var(--border);font-family:monospace;font-size:11px;color:' + (hasAttom ? 'var(--text-light)' : 'var(--red, #ef5350)') + ';">' + (hasAttom ? escapeHtml(String(attomId)) : '—') + '</td>';
      html += '</tr>';
    }

    html += '</tbody></table></div></div>';
    container.innerHTML = html;

    // Filter logic
    container.querySelectorAll('.qc-filter').forEach(function(btn) {
      btn.addEventListener('click', function() {
        container.querySelectorAll('.qc-filter').forEach(function(b) {
          b.style.background = 'var(--surface)';
          b.style.color = 'var(--text-muted)';
          b.classList.remove('active');
        });
        btn.style.background = 'var(--accent)';
        btn.style.color = '#fff';
        btn.classList.add('active');

        var filter = btn.getAttribute('data-filter');
        container.querySelectorAll('.qc-row').forEach(function(row) {
          var show = true;
          if (filter === 'no-attom') show = row.getAttribute('data-has-attom') === 'no';
          else if (filter === 'exact') show = row.getAttribute('data-confidence') === 'exact';
          else if (filter === 'multi') show = row.getAttribute('data-status') === 'multi_match';
          row.style.display = show ? '' : 'none';
        });
      });
    });

    // Apply default exact filter on load
    container.querySelectorAll('.qc-row').forEach(function(row) {
      row.style.display = row.getAttribute('data-confidence') === 'exact' ? '' : 'none';
    });

  } catch (err) {
    console.error('Failed to load QC:', err);
    container.innerHTML = '<div style="text-align:center;padding:60px;color:var(--text-muted);">Error loading quality control: ' + escapeHtml(err.message) + '</div>';
  }
}

async function loadOutreach() {
  const container = document.getElementById('outreachContent');
  if (!container) return;
  container.innerHTML = `<div class="loading"><div class="spinner"></div><div>Loading outreach opportunities...</div></div>`;

  try {
    // Fetch no_match posts with post_text for classification
    const allPosts = [];
    let offset = 0;
    const batchSize = 1000;
    let keepGoing = true;

    while (keepGoing) {
      const { data, count } = await supabaseGet('fb_deal_posts', {
        select: 'id,post_text,post_url,poster_name,group_name,captured_at,parsed_asking_price,parsed_arv,parsed_full_address,parsed_city,parsed_state,parsed_beds,parsed_baths,parsed_sqft,post_images,match_candidates,outreach_status',
        filters: [{ col: 'match_status', val: 'eq.no_match' }],
        order: 'captured_at.desc',
        limit: batchSize,
        offset,
      });
      allPosts.push(...data);
      offset += batchSize;
      if (data.length < batchSize || allPosts.length >= count) keepGoing = false;
    }

    // Classify posts
    const commentPosts = [];
    const dmPosts = [];
    const otherPosts = [];

    for (const post of allPosts) {
      const text = Array.isArray(post.post_text) ? post.post_text.join(' ') : (post.post_text || '');
      const isComment = COMMENT_PATTERN.test(text);
      const isDM = DM_PATTERN.test(text);
      if (isComment) commentPosts.push(post);
      if (isDM) dmPosts.push(post);
      if (!isComment && !isDM) otherPosts.push(post);
    }

    renderOutreach(container, commentPosts, dmPosts, otherPosts, allPosts.length);
  } catch (err) {
    console.error('Failed to load outreach:', err);
    container.innerHTML = `<div style="color:var(--red);text-align:center;padding:40px;">Failed to load outreach: ${escapeHtml(err.message)}</div>`;
  }
}

function renderOutreach(container, commentPosts, dmPosts, otherPosts, totalNoMatch) {
  const posts = outreachTab === 'comments' ? commentPosts : outreachTab === 'dms' ? dmPosts : otherPosts;
  const actionLabel = outreachTab === 'comments' ? 'Comment' : outreachTab === 'dms' ? 'DM' : 'Other';

  // Build cards HTML
  let cardsHtml = '';
  if (posts.length === 0) {
    cardsHtml = '<div style="text-align:center;padding:60px 20px;color:var(--text-muted);">' +
      '<div style="font-size:36px;margin-bottom:12px;">📭</div>' +
      '<div style="font-size:16px;font-weight:600;">No ' + actionLabel.toLowerCase() + ' opportunities found</div>' +
      '<div style="font-size:13px;margin-top:6px;">Posts will appear here.</div>' +
      '</div>';
  } else {
    const cardItems = posts.map(function(post) {
      const text = Array.isArray(post.post_text) ? post.post_text.join('\n') : (post.post_text || '');
      const posterName = cleanPosterName(post.poster_name || 'Unknown');
      const truncated = text.length > 400 ? text.substring(0, 400) + '...' : text;
      const date = formatDate(post.captured_at);
      const city = post.parsed_city || '';
      const state = post.parsed_state || '';
      const loc = [city, state].filter(Boolean).join(', ');
      const ask = post.parsed_asking_price ? '$' + Number(post.parsed_asking_price).toLocaleString() : '';

      // Look up wholesaler contact info from localStorage
      const contactKey = 'omr_contact_' + safeBtoa(posterName);
      var saved = {};
      try { saved = JSON.parse(localStorage.getItem(contactKey) || '{}'); } catch(e) {}
      const fbUrl = saved.facebook || '';
      const dmUrl = fbUrl ? fbUrl.replace(/\/$/, '') + '/messages' : '';

      // Build intro message for clipboard
      var introText = '';
      if (outreachTab === 'comments') {
        // Comments: short and simple, they'll see it on the post
        introText = 'Interested. You can email me at offmarket@rebuilt.com or DM me.';
      } else {
        // DMs and Other: reference property details since it's not tied to the post
        var details = [];
        var addr = post.parsed_full_address || loc || '';
        var beds = post.parsed_beds || '';
        var baths = post.parsed_baths || '';
        var sqft = post.parsed_sqft ? Number(post.parsed_sqft).toLocaleString() + ' sqft' : '';
        // Check match_candidates for lot size
        var mc = (post.match_candidates && post.match_candidates[0]) || {};
        var lot = mc.area_lot_sf ? (Math.round(mc.area_lot_sf / 43560 * 100) / 100) + ' acres' : '';
        if (addr) details.push(addr);
        if (beds && baths) details.push(beds + 'bd/' + baths + 'ba');
        else if (beds) details.push(beds + ' bed');
        else if (baths) details.push(baths + ' bath');
        if (sqft) details.push(sqft);
        if (lot) details.push(lot);
        if (ask) details.push(ask);
        var summary = details.length > 0 ? details.join(' | ') : 'the property you posted';
        introText = 'Hey ' + posterName + ', I\'m interested in the deal you posted:\n' + summary + '\n\nYou can reach me at offmarket@rebuilt.com. Is this still available?';
      }

      // Store intro in a data attribute (safe encoding)
      const introData = introText.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');

      var viewPostBtn = '';
      if (post.post_url) {
        var safeUrl = post.post_url.replace(/'/g, "\\'");
        viewPostBtn = '<a href="' + post.post_url + '" onclick="event.preventDefault(); openPostPopup(\'' + safeUrl + '\')" style="display:inline-flex;align-items:center;gap:4px;padding:6px 12px;border-radius:6px;background:var(--accent);color:#fff;text-decoration:none;font-size:12px;font-weight:600;cursor:pointer;transition:opacity 0.15s;" onmouseover="this.style.opacity=0.8" onmouseout="this.style.opacity=1">' +
          '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>' +
          'View Post</a>';
      }

      var dmBtn = '';
      if (dmUrl) {
        dmBtn = '<a href="' + dmUrl + '" target="_blank" style="display:inline-flex;align-items:center;gap:4px;padding:6px 12px;border-radius:6px;background:var(--purple);color:#fff;text-decoration:none;font-size:12px;font-weight:600;transition:opacity 0.15s;" onmouseover="this.style.opacity=0.8" onmouseout="this.style.opacity=1">' +
          '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><polyline points="22,6 12,13 2,6"/></svg>' +
          'Send DM</a>';
      }

      var askBadge = '';
      if (ask) {
        askBadge = '<span style="padding:6px 12px;border-radius:6px;background:rgba(34,197,94,0.1);color:var(--green);font-size:12px;font-weight:600;">' + ask + '</span>';
      }

      var copyBtn = '<button onclick="event.stopPropagation(); copyOutreachIntro(this.closest(\'.outreach-card\'))" style="display:inline-flex;align-items:center;gap:4px;padding:6px 12px;border-radius:6px;background:var(--green);color:#000;border:none;text-decoration:none;font-size:12px;font-weight:600;cursor:pointer;transition:opacity 0.15s;" onmouseover="this.style.opacity=0.8" onmouseout="this.style.opacity=1">' +
        '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>' +
        'Copy Intro</button>';

      // Check if post was already processed
      var doneMap = getOutreachDone();
      var isDone = !!(doneMap[post.id] || post.outreach_status === 'contacted');
      var doneStyle = isDone ? 'opacity:0.5;' : '';
      var doneBadge = isDone ? '<span style="display:inline-flex;align-items:center;gap:4px;padding:6px 12px;border-radius:6px;background:rgba(239,68,68,0.15);color:var(--red);font-size:12px;font-weight:600;white-space:nowrap;">' +
        '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>' +
        'Already Processed</span>' : '';

      return '<div class="outreach-card" data-postid="' + post.id + '" data-intro="' + introData + '" style="background:var(--surface);border:1px solid ' + (isDone ? 'var(--green)' : 'var(--border)') + ';border-radius:10px;padding:16px;transition:border-color 0.15s;' + doneStyle + '">' +
        '<div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px;">' +
          '<div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">' +
            '<div style="width:36px;height:36px;border-radius:8px;background:linear-gradient(135deg,var(--accent),var(--purple));display:flex;align-items:center;justify-content:center;font-size:15px;font-weight:700;color:#fff;flex-shrink:0;">' + (posterName[0] || '?').toUpperCase() + '</div>' +
            '<div>' +
              '<div style="font-weight:600;font-size:14px;">' + escapeHtml(posterName) + '</div>' +
              '<div style="font-size:11px;color:var(--text-muted);">' + escapeHtml(post.group_name || '') + (loc ? ' · ' + loc : '') + ' · ' + date + '</div>' +
            '</div>' +
          '</div>' +
          '<div style="display:flex;gap:6px;flex-shrink:0;align-items:center;">' + doneBadge + copyBtn + viewPostBtn + dmBtn + askBadge + '</div>' +
        '</div>' +
        '<pre style="white-space:pre-wrap;word-break:break-word;font-size:13px;line-height:1.6;color:var(--text-light);margin:0;font-family:inherit;background:var(--bg);border-radius:8px;padding:12px;">' + escapeHtml(truncated) + '</pre>' +
      '</div>';
    });
    cardsHtml = '<div id="outreachList" style="display:flex;flex-direction:column;gap:12px;">' + cardItems.join('') + '</div>';
  }

  container.innerHTML = `
    <div class="kpi-grid" style="grid-template-columns:repeat(4,1fr);margin-bottom:20px;">
      <div class="kpi-card red">
        <div class="kpi-glow"></div>
        <div class="kpi-value">${totalNoMatch.toLocaleString()}</div>
        <div class="kpi-label">Total No Match</div>
      </div>
      <div class="kpi-card blue">
        <div class="kpi-glow"></div>
        <div class="kpi-value">${commentPosts.length.toLocaleString()}</div>
        <div class="kpi-label">Comment Opportunities</div>
      </div>
      <div class="kpi-card purple">
        <div class="kpi-glow"></div>
        <div class="kpi-value">${dmPosts.length.toLocaleString()}</div>
        <div class="kpi-label">DM Opportunities</div>
      </div>
      <div class="kpi-card green">
        <div class="kpi-glow"></div>
        <div class="kpi-value">${(commentPosts.length + dmPosts.length).toLocaleString()}</div>
        <div class="kpi-label">Total Actionable</div>
      </div>
    </div>

    <div style="display:flex;gap:0;margin-bottom:16px;border-bottom:2px solid var(--border);">
      <button onclick="switchOutreachTab('comments')" style="
        padding:10px 24px;font-size:14px;font-weight:600;cursor:pointer;border:none;background:none;
        color:${outreachTab === 'comments' ? 'var(--accent)' : 'var(--text-muted)'};
        border-bottom:2px solid ${outreachTab === 'comments' ? 'var(--accent)' : 'transparent'};
        margin-bottom:-2px;transition:all 0.15s;
      ">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px;margin-right:6px;"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>
        Comments (${commentPosts.length})
      </button>
      <button onclick="switchOutreachTab('dms')" style="
        padding:10px 24px;font-size:14px;font-weight:600;cursor:pointer;border:none;background:none;
        color:${outreachTab === 'dms' ? 'var(--purple)' : 'var(--text-muted)'};
        border-bottom:2px solid ${outreachTab === 'dms' ? 'var(--purple)' : 'transparent'};
        margin-bottom:-2px;transition:all 0.15s;
      ">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px;margin-right:6px;"><path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><polyline points="22,6 12,13 2,6"/></svg>
        Direct Messages (${dmPosts.length})
      </button>
      <button onclick="switchOutreachTab('other')" style="
        padding:10px 24px;font-size:14px;font-weight:600;cursor:pointer;border:none;background:none;
        color:${outreachTab === 'other' ? 'var(--yellow)' : 'var(--text-muted)'};
        border-bottom:2px solid ${outreachTab === 'other' ? 'var(--yellow)' : 'transparent'};
        margin-bottom:-2px;transition:all 0.15s;
      ">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px;margin-right:6px;"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>
        Other (${otherPosts.length})
      </button>
    </div>
    ${cardsHtml}
  `;

  // Store references for tab switching
  container._commentPosts = commentPosts;
  container._dmPosts = dmPosts;
  container._otherPosts = otherPosts;
  container._totalNoMatch = totalNoMatch;
}

function copyOutreachIntro(cardEl) {
  const raw = cardEl.dataset.intro || '';
  const decoded = raw.replace(/&amp;/g, '&').replace(/&quot;/g, '"').replace(/&#39;/g, "'");
  const postId = cardEl.dataset.postid;

  function showCopied() {
    cardEl.style.borderColor = 'var(--green)';
    cardEl.style.position = 'relative';
    // Remove any existing badge
    var old = cardEl.querySelector('.copy-badge');
    if (old) old.remove();
    var badge = document.createElement('div');
    badge.className = 'copy-badge';
    badge.textContent = '✓ Intro copied to clipboard';
    badge.style.cssText = 'position:absolute;top:12px;right:16px;background:var(--green);color:#000;padding:4px 12px;border-radius:6px;font-size:12px;font-weight:600;z-index:5;';
    cardEl.appendChild(badge);

    // Mark post as processed
    if (postId) {
      markOutreachDone(postId);
      // Visually dim the card and add processed indicator
      cardEl.style.opacity = '0.5';
      var existingBadge = cardEl.querySelector('.processed-badge');
      if (!existingBadge) {
        var actionBar = cardEl.querySelector('div > div:last-child');
        if (actionBar) {
          var pb = document.createElement('span');
          pb.className = 'processed-badge';
          pb.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Already Processed';
          pb.style.cssText = 'display:inline-flex;align-items:center;gap:4px;padding:6px 12px;border-radius:6px;background:rgba(239,68,68,0.15);color:var(--red);font-size:12px;font-weight:600;white-space:nowrap;';
          actionBar.insertBefore(pb, actionBar.firstChild);
        }
      }
    }

    setTimeout(function() {
      badge.remove();
    }, 2000);
  }

  // Try modern clipboard API first, fall back to execCommand
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(decoded).then(showCopied).catch(function() {
      // Fallback for when clipboard API is denied
      fallbackCopy(decoded);
      showCopied();
    });
  } else {
    fallbackCopy(decoded);
    showCopied();
  }
}

function openPostPopup(url) {
  var w = 500;
  var h = 700;
  var left = window.screenX + window.outerWidth - w - 40;
  var top = window.screenY + 80;
  window.open(url, 'fbpost', 'width=' + w + ',height=' + h + ',left=' + left + ',top=' + top + ',scrollbars=yes,resizable=yes');
}

function fallbackCopy(text) {
  var ta = document.createElement('textarea');
  ta.value = text;
  ta.style.cssText = 'position:fixed;left:-9999px;top:-9999px;opacity:0;';
  document.body.appendChild(ta);
  ta.select();
  try { document.execCommand('copy'); } catch(e) {}
  document.body.removeChild(ta);
}

function switchOutreachTab(tab) {
  outreachTab = tab;
  const container = document.getElementById('outreachContent');
  if (container._commentPosts) {
    renderOutreach(container, container._commentPosts, container._dmPosts, container._otherPosts, container._totalNoMatch);
  }
}

// ===== RENDERING =====
function formatDate(dateStr) {
  if (!dateStr) return '-';
  const d = new Date(dateStr);
  return d.toLocaleString('en-US', {
    timeZone: 'America/Chicago',
    month: 'numeric',
    day: 'numeric',
    year: '2-digit',
    hour: 'numeric',
    minute: '2-digit',
    hour12: true,
  }).replace(',', '');
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
