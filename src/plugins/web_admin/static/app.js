const el = (tag, attrs = {}, ...kids) => {
  const n = document.createElement(tag);
  Object.entries(attrs).forEach(([k, v]) => {
    if (k === 'class') n.className = v; else if (k === 'text') n.textContent = v; else n.setAttribute(k, v);
  });
  kids.forEach(k => n.appendChild(typeof k === 'string' ? document.createTextNode(k) : k));
  return n;
};

let MODE = '…';
const DATASETS = new Map(); // name -> count
let ORDER = [];             // dataset names, most-recent first
let GRID = null;            // gridstack instance

// Global timeline state
const Timeline = { mode: 'ts', min: null, max: null, start: null, end: null, cursor: null };
function emitTimelineChange() { document.dispatchEvent(new CustomEvent('timeline-change', { detail: { ...Timeline } })); }
function setTimeline(part) { Object.assign(Timeline, part); emitTimelineChange(); }

async function fetchJSON(url) { const r = await fetch(url); return r.json(); }

async function refreshMode() {
  const m = await fetchJSON('/api/mode').catch(() => ({ mode: 'unknown' }));
  MODE = m.mode;
  document.getElementById('mode').textContent = MODE === 'attached' ? 'attached mode' : 'file mode';
  // no separate panel; a Files card is added instead
}

function datasetsChanged() { document.dispatchEvent(new Event('datasets-changed')); }

async function refreshStats() {
  const s = await fetchJSON('/api/stats').catch(() => ({ error: 'fetch failed' }));
  if (s.mode === 'file') {
    // Populate ORDER from available files (without preloading)
    try {
      const files = await fetchJSON('/api/replay/files');
      ORDER = (files.files || []).slice().sort().reverse();
    } catch { }
    // Keep any already loaded datasets at the front (maintain recency)
    for (const d of (s.datasets || [])) {
      if (!ORDER.includes(d.name)) ORDER.unshift(d.name);
      DATASETS.set(d.name, d.events);
    }
    // Dedup ORDER
    ORDER = ORDER.filter((v, i, a) => a.indexOf(v) === i);
    datasetsChanged();
  }
}

function addCard(type = 'events', dataset = null) {
  const board = document.getElementById('board');
  const id = Math.random().toString(36).slice(2);
  const head = el('div', { class: 'head' },
    el('span', { class: 'title' },
      type === 'stats' ? 'DB Stats' : (
        type === 'files' ? 'Replay Files' : (
          type === 'plot' ? 'Event Plot' : (
            type === 'timeline' ? 'Timeline' : 'Events'
          )
        )
      )
    ),
    el('div', { class: 'controls', id: `ctl-${id}` },
      type === 'files' ? el('span') : el('select', { id: `sel-${id}` }),
      el('button', { id: `size-${id}`, title: 'Fill width' }, 'Fill'),
      el('button', { id: `rm-${id}` }, 'Remove')
    )
  );
  const body = el('div', { class: 'body', id: `body-${id}` }, el('div', { class: 'muted' }, type === 'files' ? 'Load replay files' : 'Loading…'));
  const card = el('div', { class: 'card', id: `card-${id}` }, head, body);
  // gridstack wrapper
  const wrapper = el('div', { class: 'grid-stack-item' });
  const content = el('div', { class: 'grid-stack-item-content' });
  content.appendChild(card);
  wrapper.appendChild(content);
  // defaults per card type
  const cols = (GRID && GRID.opts && GRID.opts.column) ? GRID.opts.column : 12;
  let defaultW, defaultH;
  if (type === 'events') { defaultW = cols; defaultH = 4; }
  else if (type === 'stats') { defaultW = Math.min(7, cols); defaultH = 2; }
  else if (type === 'files') { defaultW = Math.min(4, cols); defaultH = 2; }
  else if (type === 'plot') { defaultW = cols; defaultH = 8; }
  else if (type === 'timeline') { defaultW = cols; defaultH = 1; }
  else { defaultW = Math.min(6, cols); defaultH = 6; }
  // attach and make into widget (v12: use makeWidget instead of addWidget(el))
  const gridEl = GRID && GRID.el ? GRID.el : document.getElementById('board');
  wrapper.setAttribute('gs-w', String(defaultW));
  wrapper.setAttribute('gs-h', String(defaultH));
  wrapper.setAttribute('gs-auto-position', 'true');
  gridEl.appendChild(wrapper);
  GRID.makeWidget(wrapper);

  // dataset selector + pagination controls + size controls
  const sel = head.querySelector(`#sel-${id}`);
  const ctl = head.querySelector(`#ctl-${id}`);
  const sizeBtn = head.querySelector(`#size-${id}`);
  // Compute maximum expansion without moving other cards
  const computeMaxExpand = () => {
    const node = wrapper.gridstackNode; if (!node) return { w: 0, h: 0 };
    const col = (GRID && GRID.opts && GRID.opts.column) ? GRID.opts.column : 12;
    const x = node.x, y = node.y, w0 = node.w, h0 = node.h;
    const rects = Array.from(document.querySelectorAll('.grid-stack-item'))
      .map(el => el.gridstackNode)
      .filter(n => n && n.el !== wrapper)
      .map(n => ({ x: n.x, y: n.y, w: n.w, h: n.h }));
    const collides = (a, b) => !(a.x + a.w <= b.x || b.x + b.w <= a.x || a.y + a.h <= b.y || b.y + b.h <= a.y);
    const free = (X, Y, W, H) => {
      if (X < 0 || Y < 0 || W < 1 || H < 1) return false;
      if (X + W > col) return false;
      const test = { x: X, y: Y, w: W, h: H };
      for (const r of rects) { if (collides(test, r)) return false; }
      return true;
    };
    // limit vertical expansion to current content bottom to avoid unbounded growth
    const maxBottom = rects.reduce((m, r) => Math.max(m, r.y + r.h), 0);
    const maxHCap = Math.max(h0, maxBottom - y);
    // expand functions
    const expandW = (W, H) => { let w = W; while (free(x, y, w + 1, H)) w++; return w; };
    const expandH = (W, H) => { let h = H; while (h + 1 <= maxHCap && free(x, y, W, h + 1)) h++; return h; };
    // try width-first
    const wf_w = expandW(w0, h0);
    const wf_h = expandH(wf_w, h0);
    // try height-first
    const hf_h = expandH(w0, h0);
    const hf_w = expandW(w0, hf_h);
    const areaWF = wf_w * wf_h, areaHF = hf_w * hf_h;
    if (areaWF >= areaHF) return { w: wf_w, h: wf_h };
    return { w: hf_w, h: hf_h };
  };
  const updateFillState = () => {
    const node = wrapper.gridstackNode; if (!node) return;
    const max = computeMaxExpand();
    sizeBtn.disabled = (node.w >= max.w && node.h >= max.h);
  };
  sizeBtn.onclick = () => {
    const node = wrapper.gridstackNode; if (!node) return;
    const max = computeMaxExpand();
    GRID.update(wrapper, { w: max.w, h: max.h });
    updateFillState();
  };
  // set initial disabled state
  setTimeout(updateFillState, 0);
  // timeline card setup
  if (type === 'timeline') {
    // controls
    const timeModeEl = el('select', { id: `tmode-${id}` });
    timeModeEl.append(el('option', { value: 'ts', text: 'Timestamp' }), el('option', { value: 'date', text: 'Date' }));
    const startIn = el('input', { type: 'number', id: `tstart-${id}`, placeholder: 'start' });
    const endIn = el('input', { type: 'number', id: `tend-${id}`, placeholder: 'end' });
    const cursorIn = el('input', { type: 'number', id: `tcursor-${id}`, placeholder: 'cursor' });
    const startDt = el('input', { type: 'datetime-local', id: `tstartd-${id}`, class: 'hidden' });
    const endDt = el('input', { type: 'datetime-local', id: `tendd-${id}`, class: 'hidden' });
    const cursorDt = el('input', { type: 'datetime-local', id: `tcursord-${id}`, class: 'hidden' });
    ctl.insertBefore(timeModeEl, ctl.lastChild);
    ctl.insertBefore(el('span', { class: 'muted', style: 'font-size:12px' }, 'Start:'), ctl.lastChild);
    ctl.insertBefore(startIn, ctl.lastChild);
    ctl.insertBefore(startDt, ctl.lastChild);
    ctl.insertBefore(el('span', { class: 'muted', style: 'font-size:12px' }, 'End:'), ctl.lastChild);
    ctl.insertBefore(endIn, ctl.lastChild);
    ctl.insertBefore(startDt, ctl.lastChild);
    ctl.insertBefore(endDt, ctl.lastChild);
    ctl.insertBefore(el('span', { class: 'muted', style: 'font-size:12px' }, 'Cursor:'), ctl.lastChild);
    ctl.insertBefore(cursorIn, ctl.lastChild);
    ctl.insertBefore(cursorDt, ctl.lastChild);
    const onMode = () => {
      const isDate = timeModeEl.value === 'date';
      startIn.classList.toggle('hidden', isDate);
      endIn.classList.toggle('hidden', isDate);
      cursorIn.classList.toggle('hidden', isDate);
      startDt.classList.toggle('hidden', !isDate);
      endDt.classList.toggle('hidden', !isDate);
      cursorDt.classList.toggle('hidden', !isDate);
      setTimeline({ mode: timeModeEl.value });
    };
    timeModeEl.onchange = onMode; onMode();

    const applyInputs = () => {
      const isDate = (Timeline.mode === 'date');
      const parseDate = d => (d ? new Date(d).getTime() : NaN);
      const s = isDate ? parseDate(startDt.value) : Number(startIn.value);
      const e = isDate ? parseDate(endDt.value) : Number(endIn.value);
      const c = isDate ? parseDate(cursorDt.value) : Number(cursorIn.value);
      const upd = {};
      if (Number.isFinite(s)) upd.start = s;
      if (Number.isFinite(e)) upd.end = e;
      if (Number.isFinite(c)) upd.cursor = c;
      // Expand min/max to include typed range so dragging covers it
      let needBoundsUpdate = false;
      const bounds = {};
      if (Number.isFinite(s) && (Timeline.min == null || s < Timeline.min)) { bounds.min = s; needBoundsUpdate = true; }
      if (Number.isFinite(e) && (Timeline.max == null || e > Timeline.max)) { bounds.max = e; needBoundsUpdate = true; }
      const payload = Object.assign({}, upd, needBoundsUpdate ? bounds : {});
      if (Object.keys(payload).length) setTimeline(payload);
    };
    startIn.onchange = applyInputs; endIn.onchange = applyInputs; cursorIn.onchange = applyInputs;
    startDt.onchange = applyInputs; endDt.onchange = applyInputs; cursorDt.onchange = applyInputs;

    // canvas
    body.innerHTML = '';
    const canv = el('canvas', { id: `tl-${id}`, style: 'width:100%; height:60px; display:block' });
    body.appendChild(canv);
    const DPR = window.devicePixelRatio || 1;
    const cssWidth = (el) => Math.max(1, Math.floor((el.getBoundingClientRect().width) || (el.parentElement ? el.parentElement.clientWidth : 300)));
    const setSize = (c, cssH) => { const cw = cssWidth(c); c.width = Math.max(1, Math.floor(cw * DPR)); c.height = Math.max(1, Math.floor(cssH * DPR)); c.style.height = cssH + 'px'; };
    const tsToX = (ts) => {
      if (Timeline.min == null || Timeline.max == null) return 0;
      const w = cssWidth(canv);
      const span = Math.max(1, Timeline.max - Timeline.min);
      return (ts - Timeline.min) / span * (w - 1);
    };
    const xToTs = (x) => {
      const w = cssWidth(canv);
      const span = Math.max(1, (Timeline.max ?? 1) - (Timeline.min ?? 0));
      return (Timeline.min ?? 0) + (x / Math.max(1, w - 1)) * span;
    };
    const draw = () => {
      const ctx = canv.getContext('2d');
      setSize(canv, 60);
      ctx.scale(DPR, DPR);
      ctx.clearRect(0, 0, canv.width, canv.height);
      const h = canv.height / DPR;
      ctx.fillStyle = '#f3f3f3'; ctx.fillRect(0, 0, canv.width / DPR, h);
      if (Timeline.start != null && Timeline.end != null) {
        const x1 = Math.floor(tsToX(Timeline.start));
        const x2 = Math.floor(tsToX(Timeline.end));
        ctx.fillStyle = 'rgba(100,150,255,0.3)';
        ctx.fillRect(Math.min(x1, x2), 0, Math.abs(x2 - x1), h);
      }
      if (Timeline.cursor != null) {
        const xc = Math.floor(tsToX(Timeline.cursor));
        ctx.strokeStyle = 'rgba(220,80,80,0.9)'; ctx.lineWidth = 2;
        ctx.beginPath(); ctx.moveTo(xc + 0.5, 0); ctx.lineTo(xc + 0.5, h); ctx.stroke();
      }
    };
    const updateInputsFromTimeline = () => {
      const isDate = (Timeline.mode === 'date');
      const fmtLocal = (ts) => { const d = new Date(ts); const P = n => String(n).padStart(2, '0'); return `${d.getFullYear()}-${P(d.getMonth() + 1)}-${P(d.getDate())}T${P(d.getHours())}:${P(d.getMinutes())}`; };
      if (isDate) {
        if (Timeline.start != null) startDt.value = fmtLocal(Timeline.start);
        if (Timeline.end != null) endDt.value = fmtLocal(Timeline.end);
        if (Timeline.cursor != null) cursorDt.value = fmtLocal(Timeline.cursor);
      } else {
        if (Timeline.start != null) startIn.value = String(Timeline.start);
        if (Timeline.end != null) endIn.value = String(Timeline.end);
        if (Timeline.cursor != null) cursorIn.value = String(Timeline.cursor);
      }
    };
    canv.addEventListener('mousedown', e => {
      const rect = canv.getBoundingClientRect();
      const x = (e.clientX - rect.left);
      const dc = (Timeline.cursor != null ? Math.abs(tsToX(Timeline.cursor) - x) : 1e9);
      const ds = (Timeline.start != null ? Math.abs(tsToX(Timeline.start) - x) : 1e9);
      const de = (Timeline.end != null ? Math.abs(tsToX(Timeline.end) - x) : 1e9);
      const minD = Math.min(dc, ds, de);
      let dragTarget = (minD === dc) ? 'cursor' : (minD === ds ? 'start' : 'end');
      // apply immediate update on mousedown
      const ts0 = Math.round(xToTs(x));
      if (dragTarget === 'cursor') setTimeline({ cursor: ts0 });
      if (dragTarget === 'start') setTimeline({ start: ts0 });
      if (dragTarget === 'end') setTimeline({ end: ts0 });
      draw(); updateInputsFromTimeline();
      const move = (ev) => {
        const xr = (ev.clientX - rect.left);
        const ts = Math.round(xToTs(xr));
        if (dragTarget === 'cursor') setTimeline({ cursor: ts });
        if (dragTarget === 'start') setTimeline({ start: ts });
        if (dragTarget === 'end') setTimeline({ end: ts });
        draw(); updateInputsFromTimeline();
      };
      const up = () => { window.removeEventListener('mousemove', move); window.removeEventListener('mouseup', up); };
      window.addEventListener('mousemove', move); window.addEventListener('mouseup', up);
    });
    document.addEventListener('timeline-change', () => { updateInputsFromTimeline(); draw(); });
    const initializeBounds = async () => {
      const src = (MODE === 'attached' ? 'attached' : (ORDER[0] || null));
      if (!src) return;
      if (MODE === 'replay' && ORDER.length === 0) { await refreshStats(); }
      const info = await fetchJSON(`/api/replay?source=${encodeURIComponent(src)}&limit=1&offset=0`).catch(() => ({}));
      const total = info.total ?? (Array.isArray(info) ? info.length : 0);
      if (!total) return;
      const latest = (info.events && info.events[0]) || (Array.isArray(info) && info[0]);
      const earliestInfo = await fetchJSON(`/api/replay?source=${encodeURIComponent(src)}&limit=1&offset=${Math.max(0, total - 1)}`).catch(() => ({}));
      const earliest = (earliestInfo.events && earliestInfo.events[0]) || (Array.isArray(earliestInfo) && earliestInfo[0]);
      if (earliest && latest) {
        const min = earliest.ts, max = latest.ts;
        let start = (Timeline.start ?? min);
        let end = (Timeline.end ?? max);
        if (!(end > start)) end = start + 1;
        let cursor = (Timeline.cursor ?? end);
        cursor = Math.max(start, Math.min(end, cursor));
        setTimeline({ mode: timeModeEl.value, min, max, start, end, cursor });
        updateInputsFromTimeline();
        draw();
      }
    };
    initializeBounds();
    document.addEventListener('datasets-changed', initializeBounds);
  }
  let offset = 0;
  const limit = 200;
  let prevBtn = null, nextBtn = null, pageLbl = null;
  if (type === 'events') {
    prevBtn = el('button', { id: `prev-${id}` }, 'Prev');
    nextBtn = el('button', { id: `next-${id}` }, 'Next');
    pageLbl = el('span', { id: `page-${id}`, class: 'muted', style: 'margin-left:6px;' });
    ctl.insertBefore(prevBtn, ctl.lastChild);
    ctl.insertBefore(nextBtn, ctl.lastChild);
    ctl.insertBefore(pageLbl, ctl.lastChild);
  }
  // plot-specific controls (global timeline is used for range)
  let modeSelEl = null, binCountEl = null, stopBtn = null, statusLbl = null;
  if (type === 'plot') {
    modeSelEl = el('select', { id: `mode-${id}` });
    modeSelEl.append(el('option', { value: 'origin', text: 'Group: Origin' }), el('option', { value: 'table', text: 'Group: Table' }));
    binCountEl = el('input', { type: 'number', min: '1', step: '1', value: '100', id: `bins-${id}`, title: 'Number of bins' });
    stopBtn = el('button', { id: `stop-${id}` }, 'Stop');
    statusLbl = el('span', { id: `status-${id}`, class: 'muted' });
    ctl.insertBefore(modeSelEl, ctl.lastChild);
    ctl.insertBefore(binCountEl, ctl.lastChild);
    ctl.insertBefore(stopBtn, ctl.lastChild);
    ctl.insertBefore(statusLbl, ctl.lastChild);
  }
  const fillSel = () => {
    if (!sel) return;
    sel.innerHTML = '';
    if (MODE === 'attached') sel.appendChild(el('option', { value: 'attached', text: 'attached' }));
    if (MODE === 'replay') {
      for (const name of ORDER) sel.appendChild(el('option', { value: name, text: name }));
    }
    if (dataset) sel.value = dataset;
    else if (MODE === 'replay' && ORDER.length) sel.value = ORDER[0];
  };
  if (sel) { fillSel(); document.addEventListener('datasets-changed', fillSel); }
  // auto re-render plot and stats on relevant control changes
  if (type === 'plot') {
    const re = () => render();
    modeSelEl && (modeSelEl.onchange = re);
    binCountEl && (binCountEl.onchange = re);
    document.addEventListener('timeline-change', re);
  }
  if (type === 'stats') {
    document.addEventListener('timeline-change', () => render());
  }

  // remove
  head.querySelector(`#rm-${id}`).onclick = () => GRID.removeWidget(wrapper);

  async function render() {
    if (type === 'files') {
      body.innerHTML = '';
      const up = el('div', { class: 'uploader row' });
      const lab = el('label', { class: 'btn' }, 'Choose Files');
      const inp = el('input', { type: 'file', id: `file-${id}`, multiple: 'multiple' });
      lab.appendChild(inp);
      const btn = el('button', { id: `refresh-${id}` }, 'Refresh exports/');
      const list = el('ul', { id: `files-${id}` });
      up.appendChild(lab); up.appendChild(btn);
      body.appendChild(up); body.appendChild(list);
      const refreshList = async () => {
        const res = await fetchJSON('/api/replay/files');
        list.innerHTML = '';
        (res.files || []).sort().reverse().forEach(name => {
          const a = el('a', { href: '#', text: name });
          a.onclick = async (e) => { e.preventDefault(); await fetch(`/api/replay/load?file=${encodeURIComponent(name)}`); ORDER.unshift(name); ORDER = [...new Set(ORDER)]; await refreshStats(); };
          list.appendChild(el('li', {}, a));
        });
      };
      btn.onclick = refreshList;
      inp.onchange = async () => {
        const f = inp.files;
        for (let i = 0; i < f.length; i++) {
          const buf = await f[i].arrayBuffer();
          const name = f[i].name.replace(/\.[^.]+$/, '');
          await fetch(`/api/replay/upload?name=${encodeURIComponent(name)}`, { method: 'POST', headers: { 'Content-Type': 'application/octet-stream' }, body: buf });
          ORDER.unshift(name);
        }
        ORDER = [...new Set(ORDER)];
        await refreshStats();
        datasetsChanged();
      };
      await refreshList();
    } else if (type === 'stats') {
      const src = sel.value || (MODE === 'attached' ? 'attached' : null);
      if (!src) { body.innerHTML = '<div class="muted">Pick a dataset</div>'; return; }
      // stats at cursor (for replay): closest snapshot before cursor; attached = live
      let s;
      if (src === 'attached') s = await fetchJSON('/api/stats');
      else {
        const at = (Timeline.cursor != null) ? `&at=${Timeline.cursor}` : '';
        s = await fetchJSON(`/api/replay_stats?source=${encodeURIComponent(src)}${at}`);
      }
      const atTs = s && s.at != null ? s.at : (Timeline.cursor ?? null);
      const iso = atTs != null ? new Date(atTs).toISOString() : '';
      body.innerHTML = `<div class="muted">at: ${atTs ?? ''} ${iso ? '(' + iso + ')' : ''}</div><pre>${JSON.stringify(s, null, 2)}</pre>`;
    } else if (type === 'timeline') {
      // timeline managed by its own listeners; nothing to fetch here
      return;
    } else {
      const src = sel.value || (MODE === 'attached' ? 'attached' : null);
      if (!src) { body.innerHTML = '<div class="muted">Pick a dataset</div>'; return; }
      if (type === 'plot') {
        body.innerHTML = '';
        const canvRaster = el('canvas', { id: `raster-${id}`, style: 'width:100%; height:200px; border-bottom:1px solid #eee; display:block' });
        const canvLine = el('canvas', { id: `line-${id}`, style: 'width:100%; height:240px; display:block' });
        body.appendChild(canvRaster); body.appendChild(canvLine);

        // allow abort of long runs and re-render on control changes
        if (!addCard._runs) addCard._runs = new Map();
        const runKey = id;
        const prev = addCard._runs.get(runKey); if (prev) prev.abort = true;
        const run = { abort: false }; addCard._runs.set(runKey, run);
        if (stopBtn) stopBtn.onclick = () => { run.abort = true; };
        const info = await fetchJSON(`/api/replay?source=${encodeURIComponent(src)}&limit=1&offset=0`);
        const total = info.total ?? (Array.isArray(info) ? info.length : 0);
        console.log('[plot] dataset=%s total=%d info=%o', src, total, info);
        if (!total) { body.innerHTML = '<div class="muted">No events.</div>'; console.warn('[plot] no events'); return; }
        const latest = (info.events && info.events[0]) || (Array.isArray(info) && info[0]);
        const earliestInfo = await fetchJSON(`/api/replay?source=${encodeURIComponent(src)}&limit=1&offset=${Math.max(0, total - 1)}`);
        const earliest = (earliestInfo.events && earliestInfo.events[0]) || (Array.isArray(earliestInfo) && earliestInfo[0]);
        console.log('[plot] earliest=%o latest=%o', earliest, latest);
        // Use global timeline range or default to dataset bounds
        let tsStart = (Timeline.start != null) ? Timeline.start : (earliest?.ts ?? 0);
        let tsEnd = (Timeline.end != null) ? Timeline.end : (latest?.ts ?? (tsStart + 1));
        if (!(tsEnd > tsStart)) { console.warn('[plot] invalid computed range, clamping', { tsStart, tsEnd }); tsEnd = tsStart + 1; }
        console.log('[plot] using range start=%d end=%d (span=%d)', tsStart, tsEnd, tsEnd - tsStart);
        const binsCount = Math.max(1, Number(binCountEl?.value || '100'));
        const binMs = Math.max(1, Math.floor((tsEnd - tsStart) / binsCount));
        const groupBy = modeSelEl ? modeSelEl.value : 'origin';

        const rctx = canvRaster.getContext('2d');
        const lctx = canvLine.getContext('2d');
        const DPR = window.devicePixelRatio || 1;
        const cssWidth = (el) => {
          const rect = el.getBoundingClientRect();
          return Math.max(1, Math.floor((rect && rect.width) ? rect.width : (el.parentElement ? el.parentElement.clientWidth : 300)));
        };
        const setSize = (c, cssH) => { const cw = cssWidth(c); c.width = Math.max(1, Math.floor(cw * DPR)); c.height = Math.max(1, Math.floor(cssH * DPR)); c.style.height = cssH + 'px'; };
        // Ensure layout has happened before measuring
        await new Promise(requestAnimationFrame);
        setSize(canvRaster, 200); setSize(canvLine, 140);
        rctx.scale(DPR, DPR); lctx.scale(DPR, DPR);
        rctx.clearRect(0, 0, canvRaster.width, canvRaster.height);
        lctx.clearRect(0, 0, canvLine.width, canvLine.height);
        console.log('[plot] canvas raster=%dx%d line=%dx%d DPR=%d', canvRaster.width, canvRaster.height, canvLine.width, canvLine.height, DPR);

        const hash = (s) => { let h = 2166136261 >>> 0; for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619); } return h >>> 0; };
        const colorFor = (key) => { const h = hash(key) % 360; return `hsl(${h},70%,45%)`; };

        const catIndex = new Map();
        const bins = new Map(); // key -> Map(binIdx->count)
        const rowH = 12;
        let categories = 0;

        const drawDot = (ts, key) => {
          const wpx = cssWidth(canvRaster);
          const x = (ts - tsStart) / (tsEnd - tsStart) * (wpx - 1);
          if (!(x >= 0 && x <= wpx)) return;
          let idx = catIndex.get(key);
          if (idx === undefined) { idx = categories++; catIndex.set(key, idx); }
          const y = idx * rowH + rowH * 0.5;
          rctx.fillStyle = colorFor(key);
          rctx.fillRect(Math.floor(x), Math.floor(y), 2, 2);
        };
        const bumpBin = (ts, key) => {
          const b = Math.floor((ts - tsStart) / binMs);
          if (b < 0 || (ts > tsEnd)) return;
          let m = bins.get(key); if (!m) { m = new Map(); bins.set(key, m); }
          m.set(b, (m.get(b) || 0) + 1);
        };

        const chunk = 10000;
        const chunks = Math.ceil(total / chunk);
        let seenCount = 0, inRangeCount = 0;
        for (let j = 0; j < chunks && !run.abort; j++) {
          const sIdx = j * chunk;
          const eIdx = Math.min((j + 1) * chunk, total);
          const lim = eIdx - sIdx;
          const off = total - eIdx;
          statusLbl && (statusLbl.textContent = `loading ${sIdx}…${eIdx} / ${total}`);
          const resp = await fetchJSON(`/api/replay?source=${encodeURIComponent(src)}&limit=${lim}&offset=${off}`);
          const rows = resp.events || resp;
          console.log('[plot] chunk %d range[%d,%d) lim=%d off=%d got=%d', j, sIdx, eIdx, lim, off, Array.isArray(rows) ? rows.length : -1);
          for (const e of rows) {
            const key = groupBy === 'origin' ? String(e.origin || e.origin_code) : ((e.tables || [])[0] || '');
            const ts = e.ts;
            seenCount++;
            if (ts >= tsStart && ts <= tsEnd) { inRangeCount++; drawDot(ts, key); bumpBin(ts, key); }
          }
        }
        console.log('[plot] totals seen=%d inRange=%d cats=%d series=%d', seenCount, inRangeCount, categories, bins.size);
        statusLbl && (statusLbl.textContent = run.abort ? 'stopped' : `rendering… (${inRangeCount} in range)`);

        // Line datasets
        const labels = [];
        const totalBins = Math.ceil((tsEnd - tsStart) / binMs);
        for (let i = 0; i < totalBins; i++) { labels.push(tsStart + i * binMs); }
        const datasets = [];
        bins.forEach((m, key) => {
          const data = labels.map((t, i) => ({ x: t, y: m.get(i) || 0 }));
          datasets.push({ label: key, data, borderColor: colorFor(key), backgroundColor: colorFor(key), pointRadius: 0, tension: 0.1 });
        });
        console.log('[plot] datasets', datasets);
        const cursorPlugin = {
          id: 'cursorPlugin20',
          afterDatasetsDraw(chart) {
            if (Timeline.cursor == null) return;
            const xScale = chart.scales.x; const area = chart.chartArea;
            if (!xScale || !area) return;
            const x = xScale.getPixelForValue(Timeline.cursor);
            const ctx = chart.ctx;
            ctx.save();
            ctx.strokeStyle = 'rgba(220,80,80,0.9)';
            ctx.lineWidth = 2;
            const y1 = area.top + 2; // a bit below the top
            const y2 = Math.min(area.bottom - 2, y1 + 20);
            ctx.beginPath(); ctx.moveTo(x, y1); ctx.lineTo(x, y2); ctx.stroke();
            ctx.restore();
          }
        };
        new Chart(lctx, {
          type: 'line',
          data: { datasets },
          options: { parsing: false, animation: false, responsive: false, maintainAspectRatio: true, scales: { x: { type: 'linear' }, y: { beginAtZero: true } }, plugins: { legend: { display: true, position: 'bottom' } } },
          plugins: [cursorPlugin]
        });
        statusLbl && (statusLbl.textContent = run.abort ? 'stopped' : 'done');
        return;
      }
      const ev = await fetchJSON(`/api/replay?source=${encodeURIComponent(src)}&limit=${limit}&offset=${offset}`);

      body.innerHTML = '';
      const rows = Array.isArray(ev) ? ev : (ev.events || []);
      const total = typeof ev === 'object' && ev && typeof ev.total === 'number' ? ev.total : rows.length;
      if (pageLbl) pageLbl.textContent = `${offset}–${offset + rows.length} / ${total}`;
      const table = el('table', { class: 'events' });
      const thead = el('thead');
      const thr = el('tr');
      const thDelta = el('th', { text: 'Δ' });
      const thId = el('th', { text: 'ID' });
      const thTime = el('th', { text: 'Time', id: `tth-${id}`, title: 'Click to toggle raw/ISO' });
      const thOrigin = el('th', { text: 'Origin' });
      const thPeer = el('th', { text: 'Peer' });
      const thPuts = el('th', { text: 'Puts' });
      const thDels = el('th', { text: 'Deletes' });
      const thTables = el('th', { text: 'Tables' });
      thr.append(thDelta, thId, thTime, thOrigin, thPeer, thPuts, thDels, thTables);
      thead.appendChild(thr); table.appendChild(thead);
      const tbody = el('tbody');
      let timeIso = false;
      const fmt = ts => timeIso ? new Date(ts).toISOString() : String(ts);
      thTime.onclick = () => { timeIso = !timeIso; renderRows(); };
      const renderRows = () => {
        tbody.innerHTML = '';
        // filter by global timeline if set
        let filtered = rows;
        if (Timeline.start != null && Timeline.end != null) filtered = filtered.filter(x => x.ts >= Timeline.start && x.ts <= Timeline.end);
        let cursorIdx = -1; let bestDiff = Infinity;
        filtered.forEach((e, idx) => {
          const tr = el('tr');
          const delta = (Timeline.cursor != null) ? (e.ts - Timeline.cursor) : '';
          tr.append(
            el('td', { text: delta === '' ? '' : String(delta) }),
            el('td', { text: String(e.id) }),
            el('td', { text: fmt(e.ts) }),
            el('td', { text: String(e.origin || e.origin_code) }),
            el('td', { text: e.peer_id }),
            el('td', { text: String(e.puts ?? e.items_count ?? 0) }),
            el('td', { text: String(e.deletes ?? 0) }),
            el('td', { text: (e.tables || []).join(', ') })
          );
          tbody.appendChild(tr);
          if (Timeline.cursor != null) { const d = Math.abs(e.ts - Timeline.cursor); if (d < bestDiff) { bestDiff = d; cursorIdx = idx; } }
        });
        if (cursorIdx >= 0) {
          const row = tbody.children[cursorIdx]; if (row) row.classList.add('cursor-line');
        }
      };
      renderRows();
      table.appendChild(tbody);
      body.appendChild(table);
      // Basic column resize grips
      let startX = 0, startW = 0, target = null;
      table.querySelectorAll('th').forEach(th => {
        th.style.position = 'relative';
        const grip = el('div', { style: 'position:absolute;right:0;top:0;width:6px;height:100%;cursor:col-resize;' });
        th.appendChild(grip);
        grip.onmousedown = (e) => { startX = e.clientX; startW = th.offsetWidth; target = th; document.body.style.userSelect = 'none'; };
      });
      document.addEventListener('mousemove', e => { if (!target) return; const dx = e.clientX - startX; target.style.width = (startW + dx) + 'px'; });
      document.addEventListener('mouseup', () => { target = null; document.body.style.userSelect = ''; });

      // update pagination buttons
      if (prevBtn) prevBtn.disabled = offset + rows.length >= total;
      if (nextBtn) nextBtn.disabled = offset === 0;

      if (prevBtn) prevBtn.onclick = async () => { offset += limit; await render(); };
      if (nextBtn) nextBtn.onclick = async () => { offset = Math.max(0, offset - limit); await render(); };

    }
  }
  sel && (sel.onchange = () => { offset = 0; render(); });
  if (type === 'events' || type === 'plot') {
    document.addEventListener('timeline-change', () => { render(); });
  }
  render();

}

async function init() {
  await refreshMode();
  await refreshStats();
  // Initialize global timeline bounds from default dataset
  async function initTimelineBounds() {
    const src = (MODE === 'attached' ? 'attached' : (ORDER[0] || null));
    if (!src) return;
    const info = await fetchJSON(`/api/replay?source=${encodeURIComponent(src)}&limit=1&offset=0`).catch(() => ({}));
    const total = info.total ?? (Array.isArray(info) ? info.length : 0);
    if (!total) return;
    const latest = (info.events && info.events[0]) || (Array.isArray(info) && info[0]);
    const earliestInfo = await fetchJSON(`/api/replay?source=${encodeURIComponent(src)}&limit=1&offset=${Math.max(0, total - 1)}`).catch(() => ({}));
    const earliest = (earliestInfo.events && earliestInfo.events[0]) || (Array.isArray(earliestInfo) && earliestInfo[0]);
    if (earliest && latest) {
      const min = earliest.ts, max = latest.ts;
      if (Timeline.min == null || Timeline.max == null) {
        setTimeline({ mode: 'ts', min, max, start: (Timeline.start ?? min), end: (Timeline.end ?? max), cursor: (Timeline.cursor ?? max) });
      }
    }
  }
  await initTimelineBounds();
  document.addEventListener('datasets-changed', initTimelineBounds);
  GRID = GridStack.init({
    handle: '.head',
    margin: 6,
    column: 12,
    float: true,
    resizable: { handles: 'e, se, s, sw, w' },
    disableOneColumnMode: false,
  }, document.getElementById('board'));

  // update Fill buttons disabled state after user resizes or when layout changes
  const refreshFillButtons = (els) => {
    const iter = els && els.length ? els : document.querySelectorAll('.grid-stack-item');
    iter.forEach(el => {
      const btn = el.querySelector('.head .controls button[id^=size-]');
      const node = el.gridstackNode;
      if (btn && node) {
        const col = (GRID && GRID.opts && GRID.opts.column) ? GRID.opts.column : 12;
        btn.disabled = node.w >= col;
      }
    });
  };
  GRID.on('resizestop', (e, el) => refreshFillButtons([el]));
  GRID.on('change', (e, items) => refreshFillButtons(items ? items.map(i => i.el) : undefined));

  // default cards
  addCard('timeline');
  addCard('files');
  addCard('stats');
  addCard('events', MODE === 'attached' ? 'attached' : (ORDER[0] || null));

  document.getElementById('add-card').onclick = () => {
    // toggle quick menu
    const actions = document.querySelector('header .actions');
    let menu = document.getElementById('add-menu');
    if (menu) { menu.remove(); return; }
    menu = el('div', { id: 'add-menu', class: 'menu' });
    const add = (label, type) => {
      const b = el('button', {}, label);
      b.onclick = () => { addCard(type); menu.remove(); };
      menu.appendChild(b);
    };
    add('Timeline', 'timeline');
    add('Events', 'events');
    add('DB Stats', 'stats');
    add('Replay Files', 'files');
    add('Event Plot', 'plot');
    actions.style.position = 'relative';
    actions.appendChild(menu);
  };
}
init();
