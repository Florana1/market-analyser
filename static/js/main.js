/* ============================================================
   QQQ Analyzer — Frontend Logic
   ============================================================ */

'use strict';

// ---- State --------------------------------------------------
let allHoldings = [];
let currentSort = { field: 'contribution', direction: 'desc' };
let refreshTimer   = null;
let countdownTimer = null;
let nextRefreshAt  = 0;
let lastFetchedAt  = null;  // ISO string of last successful data fetch

// ---- Live ET Clock (ticks every second, independent of data refresh) --------
function startLiveClock() {
  function tick() {
    const etTime = new Date().toLocaleTimeString('en-US', {
      timeZone: 'America/New_York',
      hour:   '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: true,
    });
    document.getElementById('timeInfo').textContent = etTime + ' ET';
  }
  tick();
  setInterval(tick, 1000);
}

// ---- Color Utilities ----------------------------------------
function getChangeBg(pct) {
  if (pct === null || pct === undefined) return '#4a5568';
  if (pct >  2.0) return '#1a9e5c';
  if (pct >  0.5) return '#2ea043';
  if (pct >  0.0) return '#3fb950';
  if (pct === 0)  return '#4a5568';
  if (pct > -0.5) return '#e05555';
  if (pct > -2.0) return '#c0392b';
  return '#b91c1c';
}

function getChangeClass(val) {
  if (val === null || val === undefined) return 'neutral';
  if (val > 0)  return 'pos';
  if (val < 0)  return 'neg';
  return 'neutral';
}

function getStrongClass(val) {
  if (val === null || val === undefined) return 'neutral';
  const abs = Math.abs(val);
  if (val > 0) return abs >= 2 ? 'pos-strong' : 'pos';
  if (val < 0) return abs >= 2 ? 'neg-strong' : 'neg';
  return 'neutral';
}

// ---- Formatters ---------------------------------------------
function fmtPct(val, decimals) {
  decimals = decimals !== undefined ? decimals : 2;
  if (val === null || val === undefined) return '--';
  const sign = val >= 0 ? '+' : '';
  return `${sign}${val.toFixed(decimals)}%`;
}

function fmtDollar(val) {
  if (val === null || val === undefined) return '--';
  const sign = val >= 0 ? '+' : '-';
  return `${sign}$${Math.abs(val).toFixed(2)}`;
}

function fmtPrice(val) {
  if (val === null || val === undefined) return '--';
  return `$${val.toFixed(2)}`;
}

function fmtMarketCap(val) {
  if (val === null || val === undefined) return '--';
  if (val >= 1e12) return `$${(val / 1e12).toFixed(2)}T`;
  if (val >= 1e9)  return `$${(val / 1e9).toFixed(1)}B`;
  if (val >= 1e6)  return `$${(val / 1e6).toFixed(0)}M`;
  return `$${val.toFixed(0)}`;
}

// ---- Heatmap ------------------------------------------------
function renderHeatmap(holdings) {
  const grid = document.getElementById('heatmapGrid');
  grid.innerHTML = '';

  // Top 30 by weight for heatmap
  const top = holdings
    .slice()
    .sort((a, b) => b.weight - a.weight)
    .slice(0, 30);

  if (top.length === 0) {
    grid.innerHTML = '<div style="color:var(--text-secondary);padding:20px">暂无数据</div>';
    return;
  }

  const maxWeight = top[0].weight;

  top.forEach(h => {
    const tile = document.createElement('div');
    tile.className = 'heatmap-tile';

    // Scale tile width: largest holding gets ~100px, smallest ~52px
    const ratio = maxWeight > 0 ? h.weight / maxWeight : 0.5;
    const tileW = Math.round(52 + ratio * 60); // 52px to 112px
    tile.style.width  = tileW + 'px';
    tile.style.background = getChangeBg(h.change_pct);

    tile.innerHTML = `
      <div class="tile-ticker">${escHtml(h.ticker)}</div>
      <div class="tile-change">${fmtPct(h.change_pct)}</div>
      <div class="tile-weight">${h.weight.toFixed(2)}%</div>
    `;

    // Tooltip on hover
    tile.addEventListener('mouseenter', (e) => showTooltip(e, h));
    tile.addEventListener('mousemove',  (e) => moveTooltip(e));
    tile.addEventListener('mouseleave',      hideTooltip);

    // Click: filter table to this ticker
    tile.addEventListener('click', () => {
      document.getElementById('searchBox').value = h.ticker;
      applyFiltersAndRender();
      document.querySelector('.table-section').scrollIntoView({ behavior: 'smooth' });
    });

    grid.appendChild(tile);
  });
}

// ---- Tooltip ------------------------------------------------
const tooltipEl = document.getElementById('tooltip');

function showTooltip(e, h) {
  tooltipEl.innerHTML = `
    <div class="tooltip-ticker ${getStrongClass(h.change_pct)}">${escHtml(h.ticker)}</div>
    <div style="font-size:0.75rem;color:var(--text-secondary);margin-bottom:6px">${escHtml(h.name)}</div>
    <div class="tooltip-row">权重<span>${h.weight.toFixed(4)}%</span></div>
    <div class="tooltip-row">当前价<span>${fmtPrice(h.price)}</span></div>
    <div class="tooltip-row">涨跌额<span class="${getChangeClass(h.change_dollar)}">${fmtDollar(h.change_dollar)}</span></div>
    <div class="tooltip-row">涨跌幅<span class="${getStrongClass(h.change_pct)}">${fmtPct(h.change_pct)}</span></div>
    <div class="tooltip-row">对QQQ贡献<span class="${getStrongClass(h.contribution)}">${fmtPct(h.contribution, 2)}</span></div>
  `;
  tooltipEl.classList.add('visible');
  moveTooltip(e);
}

function moveTooltip(e) {
  const x = e.clientX + 14;
  const y = e.clientY - 10;
  const w = tooltipEl.offsetWidth;
  const h = tooltipEl.offsetHeight;
  const vw = window.innerWidth;
  const vh = window.innerHeight;
  tooltipEl.style.left = (x + w > vw - 8 ? vw - w - 8 : x) + 'px';
  tooltipEl.style.top  = (y + h > vh - 8 ? y - h - 4  : y) + 'px';
}

function hideTooltip() {
  tooltipEl.classList.remove('visible');
}

// ---- Table --------------------------------------------------
function renderTable(holdings) {
  const tbody = document.getElementById('holdingsBody');
  const footer = document.getElementById('tableFooter');

  if (holdings.length === 0) {
    tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;padding:32px;color:var(--text-secondary)">无匹配结果</td></tr>';
    footer.textContent = '';
    return;
  }

  const maxAbsContrib = Math.max(...holdings.map(h => Math.abs(h.contribution || 0)));

  tbody.innerHTML = holdings.map(h => {
    const barPct = maxAbsContrib > 0 ? Math.round(Math.abs(h.contribution) / maxAbsContrib * 100) : 0;
    const barColor = getChangeBg(h.change_pct);

    return `<tr>
      <td class="col-left"><span class="ticker-cell">${escHtml(h.ticker)}</span></td>
      <td class="col-left"><span class="name-cell" title="${escHtml(h.name)}">${escHtml(h.name)}</span></td>
      <td style="text-align:right">${fmtMarketCap(h.market_cap)}</td>
      <td>${h.weight.toFixed(4)}%</td>
      <td>${fmtPrice(h.price)}</td>
      <td class="${getChangeClass(h.change_dollar)}">${fmtDollar(h.change_dollar)}</td>
      <td class="${getStrongClass(h.change_pct)}">${fmtPct(h.change_pct)}</td>
      <td>
        <div class="contrib-cell">
          <span class="${getStrongClass(h.contribution)}">${fmtPct(h.contribution, 2)}</span>
          <div class="contrib-bar-wrap">
            <div class="contrib-bar" style="width:${barPct}%;background:${barColor}"></div>
          </div>
        </div>
      </td>
    </tr>`;
  }).join('');

  footer.textContent = `共 ${holdings.length} 支 / 全部 ${allHoldings.length} 支`;
}

// ---- Sort ---------------------------------------------------
function sortHoldings(field) {
  if (currentSort.field === field) {
    currentSort.direction = currentSort.direction === 'desc' ? 'asc' : 'desc';
  } else {
    currentSort = { field, direction: 'desc' };
  }
  updateSortIcons();
  applyFiltersAndRender();
}

function updateSortIcons() {
  document.querySelectorAll('#holdingsTable th').forEach(th => {
    const f = th.dataset.sort;
    if (!f) return;
    const icon = th.querySelector('.sort-icon');
    th.classList.remove('sorted');
    if (icon) icon.textContent = '↕';
    if (f === currentSort.field) {
      th.classList.add('sorted');
      if (icon) icon.textContent = currentSort.direction === 'desc' ? '↓' : '↑';
    }
  });
}

// ---- Filters & Render ---------------------------------------
function applyFiltersAndRender() {
  const search = document.getElementById('searchBox').value.trim().toLowerCase();

  let filtered = allHoldings.filter(h => {
    return !search ||
      h.ticker.toLowerCase().includes(search) ||
      (h.name || '').toLowerCase().includes(search);
  });

  // Sort
  const dir = currentSort.direction === 'asc' ? 1 : -1;
  filtered.sort((a, b) => {
    let va = a[currentSort.field];
    let vb = b[currentSort.field];
    if (typeof va === 'string') return dir * va.localeCompare(vb || '');
    va = va !== null && va !== undefined ? va : -Infinity;
    vb = vb !== null && vb !== undefined ? vb : -Infinity;
    return dir * (va - vb);
  });

  renderTable(filtered);
}

// ---- Update QQQ Hero ----------------------------------------
function updateHero(data) {
  const qqq = data.qqq || {};
  const ms  = data.market_status || {};

  // Price
  document.getElementById('qqqPrice').textContent = fmtPrice(qqq.price);

  // Change
  const changeEl = document.getElementById('qqqChange');
  const changeTxt = `${fmtDollar(qqq.change_dollar)}  (${fmtPct(qqq.change_pct)})`;
  changeEl.textContent = changeTxt;
  changeEl.className = 'qqq-change ' + getStrongClass(qqq.change_dollar);

  // Stats
  const totalContrib = qqq.total_contribution;
  const tcEl = document.getElementById('totalContrib');
  tcEl.textContent = fmtPct(totalContrib, 2);
  tcEl.className = 'stat-value ' + getStrongClass(totalContrib);

  document.getElementById('holdingsCount').textContent =
    data.holdings ? data.holdings.length : '--';

  // Market badge
  const dot = document.getElementById('marketDot');
  dot.className = 'market-dot dot-' + (ms.session || 'closed');
  document.getElementById('marketLabel').textContent = ms.label || '未知';
}


// ---- Fetch Data ---------------------------------------------
async function fetchData() {
  const btn = document.getElementById('refreshBtn');
  btn.textContent = '刷新中...';
  btn.disabled = true;

  try {
    const resp = await fetch('/api/qqq');
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();

    if (data.error) throw new Error(data.error);

    // Update hero
    updateHero(data);

    // Update holdings
    allHoldings = data.holdings || [];
    renderHeatmap(allHoldings);
    applyFiltersAndRender();

    // Record last fetch time for countdown display
    lastFetchedAt = data.fetched_at ? new Date(data.fetched_at).toLocaleTimeString('zh-CN') : null;

    // Schedule next refresh
    const interval = (data.market_status?.refresh_interval || 120) * 1000;
    scheduleRefresh(interval);

    // Show content
    document.getElementById('loadingOverlay').style.display = 'none';
    document.getElementById('mainContent').style.display = 'block';

  } catch (err) {
    console.error('fetchData error:', err);
    document.getElementById('loadingOverlay').innerHTML =
      `<div style="color:#e05555;font-size:1rem;text-align:center;padding:20px">
         数据加载失败<br>
         <small style="color:var(--text-secondary)">${escHtml(err.message)}</small><br><br>
         <button onclick="fetchData()" style="margin-top:8px" class="refresh-btn">重试</button>
       </div>`;
  } finally {
    btn.textContent = '立即刷新';
    btn.disabled = false;
  }
}

function scheduleRefresh(intervalMs) {
  if (refreshTimer)   clearTimeout(refreshTimer);
  if (countdownTimer) clearInterval(countdownTimer);
  nextRefreshAt = Date.now() + intervalMs;
  refreshTimer = setTimeout(fetchData, intervalMs);

  countdownTimer = setInterval(() => {
    const secs = Math.max(0, Math.round((nextRefreshAt - Date.now()) / 1000));
    const updatedPart = lastFetchedAt ? `更新于 ${lastFetchedAt}  |  ` : '';
    document.getElementById('countdown').textContent =
      secs > 0 ? `${updatedPart}${secs}s 后刷新` : updatedPart.trimEnd();
  }, 1000);
}

// ---- XSS Helpers --------------------------------------------
function escHtml(str) {
  if (str === null || str === undefined) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function escAttr(str) {
  return escHtml(str);
}

// ---- Init ---------------------------------------------------
document.addEventListener('DOMContentLoaded', () => {
  // Sort headers
  document.querySelectorAll('#holdingsTable th[data-sort]').forEach(th => {
    th.addEventListener('click', () => sortHoldings(th.dataset.sort));
  });

  // Search / filter
  document.getElementById('searchBox').addEventListener('input', applyFiltersAndRender);

  // Refresh button — forces cache clear on server, then refetches
  document.getElementById('refreshBtn').addEventListener('click', async () => {
    try {
      await fetch('/api/refresh', { method: 'POST' });
    } catch (_) {}
    fetchData();
  });

  // Start live ET clock immediately (independent of data refresh)
  startLiveClock();

  // Initial load
  fetchData();
});
