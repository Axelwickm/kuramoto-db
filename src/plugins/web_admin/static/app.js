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
    el('span', { class: 'title' }, type === 'stats' ? 'DB Stats' : (type === 'files' ? 'Replay Files' : 'Events')),
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
    const collides = (a,b)=> !(a.x + a.w <= b.x || b.x + b.w <= a.x || a.y + a.h <= b.y || b.y + b.h <= a.y);
    const free = (X,Y,W,H)=>{
      if (X < 0 || Y < 0 || W < 1 || H < 1) return false;
      if (X + W > col) return false;
      const test = { x:X, y:Y, w:W, h:H };
      for (const r of rects){ if (collides(test, r)) return false; }
      return true;
    };
    // limit vertical expansion to current content bottom to avoid unbounded growth
    const maxBottom = rects.reduce((m,r)=> Math.max(m, r.y + r.h), 0);
    const maxHCap = Math.max(h0, maxBottom - y);
    // expand functions
    const expandW = (W,H)=>{ let w=W; while (free(x,y,w+1,H)) w++; return w; };
    const expandH = (W,H)=>{ let h=H; while (h+1 <= maxHCap && free(x,y,W,h+1)) h++; return h; };
    // try width-first
    const wf_w = expandW(w0,h0);
    const wf_h = expandH(wf_w,h0);
    // try height-first
    const hf_h = expandH(w0,h0);
    const hf_w = expandW(w0,hf_h);
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
  // plot-specific controls
  let modeSelEl=null, binCountEl=null, timeModeEl=null, startNumEl=null, endNumEl=null, startDateEl=null, endDateEl=null, stopBtn=null, statusLbl=null;
  if (type === 'plot') {
    modeSelEl = el('select', {id:`mode-${id}`});
    modeSelEl.append(el('option',{value:'origin', text:'Group: Origin'}), el('option',{value:'table', text:'Group: Table'}));
    binCountEl = el('input', {type:'number', min:'1', step:'1', value:'100', id:`bins-${id}`, title:'Number of bins'});
    timeModeEl = el('select', {id:`tmode-${id}`});
    timeModeEl.append(el('option',{value:'ts', text:'Timestamp'}), el('option',{value:'date', text:'Date'}));
    startNumEl = el('input', {type:'number', id:`start-${id}`, placeholder:'start'});
    endNumEl = el('input', {type:'number', id:`end-${id}`, placeholder:'end'});
    startDateEl = el('input', {type:'datetime-local', id:`startd-${id}`, class:'hidden'});
    endDateEl = el('input', {type:'datetime-local', id:`endd-${id}`, class:'hidden'});
    stopBtn = el('button', {id:`stop-${id}`}, 'Stop');
    statusLbl = el('span', {id:`status-${id}`, class:'muted'});
    ctl.insertBefore(modeSelEl, ctl.lastChild);
    ctl.insertBefore(binCountEl, ctl.lastChild);
    ctl.insertBefore(timeModeEl, ctl.lastChild);
    ctl.insertBefore(startNumEl, ctl.lastChild);
    ctl.insertBefore(endNumEl, ctl.lastChild);
    ctl.insertBefore(startDateEl, ctl.lastChild);
    ctl.insertBefore(endDateEl, ctl.lastChild);
    ctl.insertBefore(stopBtn, ctl.lastChild);
    ctl.insertBefore(statusLbl, ctl.lastChild);
    timeModeEl.onchange = ()=>{
      const isDate = timeModeEl.value === 'date';
      startNumEl.classList.toggle('hidden', isDate);
      endNumEl.classList.toggle('hidden', isDate);
      startDateEl.classList.toggle('hidden', !isDate);
      endDateEl.classList.toggle('hidden', !isDate);
    };
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
  // auto re-render plot on control changes
  if (type === 'plot') {
    const re = ()=> render();
    modeSelEl && (modeSelEl.onchange = re);
    binCountEl && (binCountEl.onchange = re);
    timeModeEl && (timeModeEl.onchange = re);
    startNumEl && (startNumEl.onchange = re);
    endNumEl && (endNumEl.onchange = re);
    startDateEl && (startDateEl.onchange = re);
    endDateEl && (endDateEl.onchange = re);
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
      const endpoint = src === 'attached' ? '/api/stats' : `/api/replay_stats?source=${encodeURIComponent(src)}`;
      const s = await fetchJSON(endpoint);
      body.innerHTML = `<pre>${JSON.stringify(s, null, 2)}</pre>`;
    } else {
      const src = sel.value || (MODE === 'attached' ? 'attached' : null);
      if (!src) { body.innerHTML = '<div class="muted">Pick a dataset</div>'; return; }
      if (type === 'plot') {
        body.innerHTML = '';
        const canvRaster = el('canvas', {id:`raster-${id}`, style:'width:100%; height:200px; border-bottom:1px solid #eee; display:block'});
        const canvLine = el('canvas', {id:`line-${id}`, style:'width:100%; height:240px; display:block'});
        body.appendChild(canvRaster); body.appendChild(canvLine);

        // allow abort of long runs and re-render on control changes
        if (!addCard._runs) addCard._runs = new Map();
        const runKey = id;
        const prev = addCard._runs.get(runKey); if (prev) prev.abort = true;
        const run = { abort:false }; addCard._runs.set(runKey, run);
        if (stopBtn) stopBtn.onclick = ()=>{ run.abort = true; };
        const info = await fetchJSON(`/api/replay?source=${encodeURIComponent(src)}&limit=1&offset=0`);
        const total = info.total ?? (Array.isArray(info)? info.length : 0);
        console.log('[plot] dataset=%s total=%d info=%o', src, total, info);
        if (!total){ body.innerHTML = '<div class="muted">No events.</div>'; console.warn('[plot] no events'); return; }
        const latest = (info.events && info.events[0]) || (Array.isArray(info) && info[0]);
        const earliestInfo = await fetchJSON(`/api/replay?source=${encodeURIComponent(src)}&limit=1&offset=${Math.max(0,total-1)}`);
        const earliest = (earliestInfo.events && earliestInfo.events[0]) || (Array.isArray(earliestInfo) && earliestInfo[0]);
        console.log('[plot] earliest=%o latest=%o', earliest, latest);
        const isDate = timeModeEl && timeModeEl.value === 'date';
        const parseDate = d=> (d? new Date(d).getTime() : NaN);
        // If no manual values set, default the inputs to file range for visibility
        const fmtLocal = (ts)=>{
          const d = new Date(ts);
          const pad = n=> String(n).padStart(2,'0');
          return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
        };
        if (isDate) {
          if (startDateEl && !startDateEl.value && earliest?.ts != null) startDateEl.value = fmtLocal(earliest.ts);
          if (endDateEl && !endDateEl.value && latest?.ts != null) endDateEl.value = fmtLocal(latest.ts);
        } else {
          if (startNumEl && !startNumEl.value && earliest?.ts != null) startNumEl.value = String(earliest.ts);
          if (endNumEl && !endNumEl.value && latest?.ts != null) endNumEl.value = String(latest.ts);
        }
        const startV = isDate ? parseDate(startDateEl?.value) : Number(startNumEl?.value);
        const endV = isDate ? parseDate(endDateEl?.value) : Number(endNumEl?.value);
        console.log('[plot] input startV=%o endV=%o isDate=%o', startV, endV, isDate);
        let tsStart = Number.isFinite(startV) && startV>0 ? startV : (earliest?.ts ?? 0);
        let tsEnd = Number.isFinite(endV) && endV>0 ? endV : (latest?.ts ?? (tsStart+1));
        if (!(tsEnd > tsStart)) { console.warn('[plot] invalid computed range, clamping', {tsStart, tsEnd}); tsEnd = tsStart + 1; }
        console.log('[plot] using range start=%d end=%d (span=%d)', tsStart, tsEnd, tsEnd - tsStart);
        const binsCount = Math.max(1, Number(binCountEl?.value || '100'));
        const binMs = Math.max(1, Math.floor((tsEnd - tsStart) / binsCount));
        const groupBy = modeSelEl ? modeSelEl.value : 'origin';

        const rctx = canvRaster.getContext('2d');
        const lctx = canvLine.getContext('2d');
        const DPR = window.devicePixelRatio || 1;
        const cssWidth = (el)=> {
          const rect = el.getBoundingClientRect();
          return Math.max(1, Math.floor((rect && rect.width) ? rect.width : (el.parentElement ? el.parentElement.clientWidth : 300)));
        };
        const setSize = (c, cssH)=>{ const cw = cssWidth(c); c.width = Math.max(1, Math.floor(cw*DPR)); c.height = Math.max(1, Math.floor(cssH*DPR)); c.style.height = cssH+'px'; };
        // Ensure layout has happened before measuring
        await new Promise(requestAnimationFrame);
        setSize(canvRaster, 200); setSize(canvLine, 240);
        rctx.scale(DPR, DPR); lctx.scale(DPR, DPR);
        rctx.clearRect(0,0,canvRaster.width, canvRaster.height);
        lctx.clearRect(0,0,canvLine.width, canvLine.height);
        console.log('[plot] canvas raster=%dx%d line=%dx%d DPR=%d', canvRaster.width, canvRaster.height, canvLine.width, canvLine.height, DPR);

        const hash = (s)=>{ let h=2166136261>>>0; for(let i=0;i<s.length;i++){ h^=s.charCodeAt(i); h=Math.imul(h,16777619);} return h>>>0; };
        const colorFor = (key)=>{ const h=hash(key)%360; return `hsl(${h},70%,45%)`; };

        const catIndex = new Map();
        const bins = new Map(); // key -> Map(binIdx->count)
        const rowH = 12;
        let categories = 0;

        const drawDot = (ts, key)=>{
          const wpx = cssWidth(canvRaster);
          const x = (ts - tsStart) / (tsEnd - tsStart) * (wpx - 1);
          if (!(x >= 0 && x <= wpx)) return;
          let idx = catIndex.get(key);
          if (idx === undefined){ idx = categories++; catIndex.set(key, idx); }
          const y = idx * rowH + rowH*0.5;
          rctx.fillStyle = colorFor(key);
          rctx.fillRect(Math.floor(x), Math.floor(y), 2, 2);
        };
        const bumpBin = (ts, key)=>{
          const b = Math.floor((ts - tsStart) / binMs);
          if (b < 0 || (ts > tsEnd)) return;
          let m = bins.get(key); if (!m){ m = new Map(); bins.set(key, m); }
          m.set(b, (m.get(b)||0)+1);
        };

        const chunk = 10000;
        const chunks = Math.ceil(total / chunk);
        let seenCount = 0, inRangeCount = 0;
        for (let j=0;j<chunks && !run.abort;j++){
          const sIdx = j*chunk;
          const eIdx = Math.min((j+1)*chunk, total);
          const lim = eIdx - sIdx;
          const off = total - eIdx;
          statusLbl && (statusLbl.textContent = `loading ${sIdx}…${eIdx} / ${total}`);
          const resp = await fetchJSON(`/api/replay?source=${encodeURIComponent(src)}&limit=${lim}&offset=${off}`);
          const rows = resp.events || resp;
          console.log('[plot] chunk %d range[%d,%d) lim=%d off=%d got=%d', j, sIdx, eIdx, lim, off, Array.isArray(rows)? rows.length : -1);
          for (const e of rows){
            const key = groupBy === 'origin' ? String(e.origin || e.origin_code) : ((e.tables||[])[0] || '');
            const ts = e.ts;
            seenCount++;
            if (ts >= tsStart && ts <= tsEnd){ inRangeCount++; drawDot(ts, key); bumpBin(ts, key); }
          }
        }
        console.log('[plot] totals seen=%d inRange=%d cats=%d series=%d', seenCount, inRangeCount, categories, bins.size);
        statusLbl && (statusLbl.textContent = run.abort? 'stopped' : `rendering… (${inRangeCount} in range)`);

        // Line datasets
        const labels = [];
        const totalBins = Math.ceil((tsEnd - tsStart) / binMs);
        for (let i=0;i<totalBins;i++){ labels.push(tsStart + i*binMs); }
        const datasets = [];
        bins.forEach((m, key)=>{
          const data = labels.map((t,i)=> ({ x: t, y: m.get(i)||0 }));
          datasets.push({ label: key, data, borderColor: colorFor(key), backgroundColor: colorFor(key), pointRadius: 0, tension: 0.1 });
        });
        console.log('[plot] datasets', datasets);
        new Chart(lctx, { type: 'line', data: { datasets }, options: { parsing:false, animation:false, scales: { x: { type:'linear' }, y: { beginAtZero:true } }, plugins:{ legend:{ display:true, position:'bottom' } } } });
        statusLbl && (statusLbl.textContent = run.abort? 'stopped' : 'done');
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
      const thId = el('th', { text: 'ID' });
      const thTime = el('th', { text: 'Time', id: `tth-${id}`, title: 'Click to toggle raw/ISO' });
      const thOrigin = el('th', { text: 'Origin' });
      const thPeer = el('th', { text: 'Peer' });
      const thPuts = el('th', { text: 'Puts' });
      const thDels = el('th', { text: 'Deletes' });
      const thTables = el('th', { text: 'Tables' });
      thr.append(thId, thTime, thOrigin, thPeer, thPuts, thDels, thTables);
      thead.appendChild(thr); table.appendChild(thead);
      const tbody = el('tbody');
      let timeIso = false;
      const fmt = ts => timeIso ? new Date(ts).toISOString() : String(ts);
      thTime.onclick = () => { timeIso = !timeIso; renderRows(); };
      const renderRows = () => {
        tbody.innerHTML = '';
        rows.forEach(e => {
          const tr = el('tr');
          tr.append(
            el('td', { text: String(e.id) }),
            el('td', { text: fmt(e.ts) }),
            el('td', { text: String(e.origin || e.origin_code) }),
            el('td', { text: e.peer_id }),
            el('td', { text: String(e.puts ?? e.items_count ?? 0) }),
            el('td', { text: String(e.deletes ?? 0) }),
            el('td', { text: (e.tables || []).join(', ') })
          );
          tbody.appendChild(tr);
        });
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
  render();

}

async function init() {
  await refreshMode();
  await refreshStats();
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
  addCard('files');
  addCard('stats');
  addCard('events', MODE === 'attached' ? 'attached' : (ORDER[0] || null));

  document.getElementById('add-card').onclick = () => {
    // toggle quick menu
    const actions = document.querySelector('header .actions');
    let menu = document.getElementById('add-menu');
    if (menu) { menu.remove(); return; }
    menu = el('div', {id:'add-menu', class:'menu'});
    const add = (label, type)=>{
      const b = el('button', {}, label);
      b.onclick = ()=>{ addCard(type); menu.remove(); };
      menu.appendChild(b);
    };
    add('Events', 'events');
    add('DB Stats', 'stats');
    add('Replay Files', 'files');
    add('Event Plot', 'plot');
    actions.style.position = 'relative';
    actions.appendChild(menu);
  };
}
init();
