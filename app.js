// ===== CONFIG (loaded from localStorage) =====
const SUPABASE_URL = localStorage.getItem('omr_supabase_url') || '';
const SUPABASE_ANON_KEY = localStorage.getItem('omr_supabase_key') || '';
const GMAPS_KEY = localStorage.getItem('omr_gmaps_key') || '';

const PAGE_SIZE = 50;
let currentPage = 0; // Supabase uses offset, not page number
let currentFilter = 'matches';
let allDeals = [];
let totalCount = 0;

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

  // Extract total count from content-range header
  const range = resp.headers.get('content-range');
  let count = 0;
  if (range) {
    const match = range.match(/\/(\d+)/);
    if (match) count = parseInt(match[1]);
  }

  const data = await resp.json();
  return { data, count };
}

function statusFilters(filter) {
  if (filter === 'matches') {
    return [{ col: 'match_status', val: 'in.(matched,multi_match,confirmed)' }];
  }
  if (filter && filter !== 'all') {
    return [{ col: 'match_status', val: `eq.${filter}` }];
  }
  // 'all' — exclude pending to show only processed
  return [{ col: 'match_status', val: 'neq.pending' }];
}

// ===== DATA LOADING =====
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

    allDeals = data;
    totalCount = count;
    renderDeals();
    renderPagination();

    document.getElementById('loadingState').style.display = 'none';
    document.getElementById('dealsTable').style.display = '';
    document.getElementById('dealCount').textContent = `${count} deal${count !== 1 ? 's' : ''}`;
  } catch (err) {
    console.error('Failed to load deals:', err);
    document.getElementById('loadingState').innerHTML =
      `<div style="color:var(--red)">Failed to load deals: ${escapeHtml(err.message)}</div>`;
  }
}

async function loadKPIs() {
  try {
    // Get counts for each status
    const statuses = ['matched', 'multi_match', 'confirmed', 'no_match', 'pending'];
    const counts = {};

    // Get total
    const { count: total } = await supabaseGet('fb_deal_posts', {
      select: 'id',
      limit: 0,
    });
    counts.total = total;

    // Get each status count in parallel
    await Promise.all(statuses.map(async (s) => {
      const { count } = await supabaseGet('fb_deal_posts', {
        select: 'id',
        filters: [{ col: 'match_status', val: `eq.${s}` }],
        limit: 0,
      });
      counts[s] = count;
    }));

    const processed = (counts.total || 0) - (counts.pending || 0);

    document.getElementById('kpiBar').innerHTML = `
      <div class="kpi blue"><div class="value">${counts.total || 0}</div><div class="label">Total</div></div>
      <div class="kpi purple"><div class="value">${processed}</div><div class="label">Processed</div></div>
      <div class="kpi green"><div class="value">${counts.matched || 0}</div><div class="label">Matched</div></div>
      <div class="kpi yellow"><div class="value">${counts.multi_match || 0}</div><div class="label">Multi</div></div>
      <div class="kpi red"><div class="value">${counts.no_match || 0}</div><div class="label">No Match</div></div>
      <div class="kpi"><div class="value">${counts.confirmed || 0}</div><div class="label">Confirmed</div></div>
    `;
  } catch (err) {
    console.error('Failed to load KPIs:', err);
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
  const street = getBestAddress(deal);
  if (!street) return '-';
  const candidates = deal.match_candidates || [];
  const top = candidates[0];
  const city = top?.property_address_city || deal.parsed_city || '';
  const state = top?.property_address_state || deal.parsed_state || '';
  const zip = top?.property_address_zip || deal.parsed_zip || '';
  const parts = [street.toUpperCase()];
  if (city) parts.push(city);
  const stateZip = [state, zip].filter(Boolean).join(' ');
  if (stateZip) parts.push(stateZip);
  return parts.join(', ');
}

function addressColor(deal) {
  if (deal.match_status === 'matched' || deal.match_status === 'confirmed') return '#16a34a';
  if (deal.match_status === 'multi_match') return '#ca8a04';
  if (deal.match_status === 'no_match') return '#dc2626';
  return '#94a3b8';
}

function parsedAddress(deal) {
  const parts = [];
  if (deal.parsed_full_address) return deal.parsed_full_address;
  const street = deal.parsed_street_name || '?';
  const city = deal.parsed_city || '?';
  const state = deal.parsed_state || '?';
  const zip = deal.parsed_zip || '?';
  return `${street}, ${city}, ${state} ${zip}`;
}

function renderDeals() {
  const tbody = document.getElementById('dealsBody');
  tbody.innerHTML = '';

  for (const deal of allDeals) {
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
      <td style="text-align:center;font-weight:600;color:${deal.match_count === 1 ? '#16a34a' : deal.match_count > 1 ? '#ca8a04' : '#94a3b8'}">${deal.match_count || '-'}</td>
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
            </div>
          </div>
        </li>`;
      }).join('')}</ul>`
    : '<p style="color:#94a3b8;font-size:13px;">No candidates found</p>';

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
function renderPagination() {
  const totalPages = Math.ceil(totalCount / PAGE_SIZE) || 1;
  const page = currentPage + 1;
  const el = document.getElementById('pagination');
  el.innerHTML = `
    <button ${page <= 1 ? 'disabled' : ''} onclick="goPage(${currentPage - 1})">&larr; Prev</button>
    <span class="page-info">Page ${page} of ${totalPages} &middot; ${totalCount} deals</span>
    <button ${page >= totalPages ? 'disabled' : ''} onclick="goPage(${currentPage + 1})">Next &rarr;</button>
  `;
}

function goPage(page) {
  currentPage = page;
  loadDeals();
  window.scrollTo(0, 0);
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
          <a href="${mapsLink}" target="_blank" style="color:var(--accent);text-decoration:none;">${escapeHtml(candidate.property_address_full || 'Unknown')} &#8599;</a>
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
      <h2 style="margin-bottom:16px;font-size:18px;">Settings</h2>
      <div style="display:flex;flex-direction:column;gap:12px;">
        <label style="font-size:13px;font-weight:600;color:var(--text-muted);">Supabase URL
          <input type="text" id="setSupabaseUrl" value="${escapeHtml(localStorage.getItem('omr_supabase_url') || '')}" placeholder="https://xxx.supabase.co" style="width:100%;padding:8px 12px;border:1px solid var(--border);border-radius:6px;font-size:13px;font-family:inherit;margin-top:4px;">
        </label>
        <label style="font-size:13px;font-weight:600;color:var(--text-muted);">Supabase Anon Key
          <input type="text" id="setSupabaseKey" value="${escapeHtml(localStorage.getItem('omr_supabase_key') || '')}" placeholder="eyJhbG..." style="width:100%;padding:8px 12px;border:1px solid var(--border);border-radius:6px;font-size:13px;font-family:inherit;margin-top:4px;">
        </label>
        <label style="font-size:13px;font-weight:600;color:var(--text-muted);">Google Maps API Key <span style="font-weight:400;color:var(--text-light)">(optional, for Street View)</span>
          <input type="text" id="setGmapsKey" value="${escapeHtml(localStorage.getItem('omr_gmaps_key') || '')}" placeholder="AIzaSy..." style="width:100%;padding:8px 12px;border:1px solid var(--border);border-radius:6px;font-size:13px;font-family:inherit;margin-top:4px;">
        </label>
      </div>
      <div style="display:flex;gap:10px;margin-top:20px;justify-content:flex-end;">
        <button onclick="document.getElementById('settingsOverlay').remove()" style="padding:8px 20px;background:var(--surface);border:1px solid var(--border);border-radius:6px;font-size:13px;cursor:pointer;font-family:inherit;">Cancel</button>
        <button onclick="saveSettings()" style="padding:8px 20px;background:var(--accent);color:#fff;border:none;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer;font-family:inherit;">Save & Reload</button>
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
    document.getElementById('loadingState').innerHTML = `
      <div style="max-width:400px;margin:0 auto;text-align:center;">
        <div style="font-size:18px;font-weight:700;margin-bottom:8px;">Welcome to Off Market Radar</div>
        <div style="color:var(--text-muted);margin-bottom:20px;">Configure your Supabase connection to get started.</div>
        <button onclick="openSettings()" style="padding:10px 24px;background:var(--accent);color:#fff;border:none;border-radius:6px;font-size:14px;font-weight:600;cursor:pointer;font-family:inherit;">Setup Connection</button>
      </div>
    `;
    return;
  }

  loadKPIs();
  loadDeals();
  document.getElementById('lastSync').textContent = `Loaded ${new Date().toLocaleTimeString()}`;
}

init();
