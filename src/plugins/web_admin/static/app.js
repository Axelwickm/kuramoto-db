const el = (tag, attrs={}, ...kids) => {
  const n = document.createElement(tag);
  Object.entries(attrs).forEach(([k,v]) => {
    if (k === 'class') n.className = v; else if (k === 'text') n.textContent = v; else n.setAttribute(k,v);
  });
  kids.forEach(k => n.appendChild(typeof k === 'string' ? document.createTextNode(k) : k));
  return n;
};

let MODE = '…';
const DATASETS = new Map(); // name -> count
let ORDER = [];             // dataset names, most-recent first
let DRAG_ID = null;         // currently dragged card id

async function fetchJSON(url){ const r = await fetch(url); return r.json(); }

async function refreshMode(){
  const m = await fetchJSON('/api/mode').catch(()=>({mode:'unknown'}));
  MODE = m.mode;
  document.getElementById('mode').textContent = MODE === 'attached' ? 'attached mode' : 'file mode';
  // no separate panel; a Files card is added instead
}

function datasetsChanged(){ document.dispatchEvent(new Event('datasets-changed')); }

async function refreshStats(){
  const s = await fetchJSON('/api/stats').catch(()=>({error:'fetch failed'}));
  if (s.mode === 'file') {
    // Merge into DATASETS, keep ORDER with new names first
    const incoming = (s.datasets||[]).map(d => d.name);
    for (const d of incoming){ if (!DATASETS.has(d)) ORDER.unshift(d); }
    for (const d of s.datasets||[]) { DATASETS.set(d.name, d.events); }
    // Dedup ORDER
    ORDER = ORDER.filter((v,i,a)=>a.indexOf(v)===i);
    datasetsChanged();
  }
}

function addCard(type='events', dataset=null){
  const board = document.getElementById('board');
  const id = Math.random().toString(36).slice(2);
  const head = el('div', {class:'head', draggable:'true'},
    el('span', {class:'title'}, type === 'stats' ? 'DB Stats' : (type==='files'?'Replay Files':'Events')),
    el('div', {class:'controls'},
      type==='files'? el('span') : el('select', {id:`sel-${id}`}),
      el('button', {id:`rm-${id}`}, 'Remove')
    )
  );
  const body = el('div', {class:'body', id:`body-${id}`}, el('div',{class:'muted'}, type==='files'?'Load replay files':'Loading…'));
  const card = el('div', {class:'card', id:`card-${id}`}, head, body);
  if (type==='files') board.insertBefore(card, board.firstChild); else board.appendChild(card);

  // dataset selector
  const sel = head.querySelector(`#sel-${id}`);
  const fillSel = ()=>{
    if (!sel) return;
    sel.innerHTML = '';
    if (MODE === 'attached') sel.appendChild(el('option',{value:'attached', text:'attached'}));
    if (MODE === 'replay') {
      for (const name of ORDER) sel.appendChild(el('option',{value:name, text:name}));
    }
    if (dataset) sel.value = dataset;
    else if (MODE==='replay' && ORDER.length) sel.value = ORDER[0];
  };
  if (sel) { fillSel(); document.addEventListener('datasets-changed', fillSel); }

  // remove
  head.querySelector(`#rm-${id}`).onclick = ()=> card.remove();

  async function render(){
    if (type === 'files') {
      body.innerHTML = '';
      const up = el('div', {class:'uploader row'});
      const lab = el('label', {class:'btn'}, 'Choose Files');
      const inp = el('input', {type:'file', id:`file-${id}`, multiple:'multiple'});
      lab.appendChild(inp);
      const btn = el('button', {id:`refresh-${id}`}, 'Refresh exports/');
      const list = el('ul', {id:`files-${id}`});
      up.appendChild(lab); up.appendChild(btn);
      body.appendChild(up); body.appendChild(list);
      const refreshList = async ()=>{
        const res = await fetchJSON('/api/replay/files');
        list.innerHTML='';
        (res.files||[]).sort().reverse().forEach(name => {
          const a = el('a',{href:'#',text:name});
          a.onclick = async (e)=>{ e.preventDefault(); await fetch(`/api/replay/load?file=${encodeURIComponent(name)}`); ORDER.unshift(name); ORDER=[...new Set(ORDER)]; await refreshStats(); };
          list.appendChild(el('li',{},a));
        });
      };
      btn.onclick = refreshList;
      inp.onchange = async ()=>{
        const f = inp.files;
        for (let i=0;i<f.length;i++){
          const buf = await f[i].arrayBuffer();
          const name = f[i].name.replace(/\.[^.]+$/,'');
          await fetch(`/api/replay/upload?name=${encodeURIComponent(name)}`, { method:'POST', headers:{'Content-Type':'application/octet-stream'}, body:buf });
          ORDER.unshift(name);
        }
        ORDER=[...new Set(ORDER)];
        await refreshStats();
        datasetsChanged();
      };
      await refreshList();
    } else if (type === 'stats') {
      if (MODE !== 'attached') { body.innerHTML = '<div class="muted">No DB attached.</div>'; return; }
      const s = await fetchJSON('/api/stats');
      body.innerHTML = `<pre>${JSON.stringify(s,null,2)}</pre>`;
    } else {
      const src = sel.value || (MODE==='attached'?'attached':null);
      if (!src){ body.innerHTML = '<div class="muted">Pick a dataset</div>'; return; }
      const ev = await fetchJSON(`/api/replay?source=${encodeURIComponent(src)}&limit=200`);
      
body.innerHTML = '';
      const table = el('table', {class:'events'});
      const thead = el('thead');
      const thr = el('tr');
      const thId = el('th', {text:'ID'});
      const thTime = el('th', {text:'Time', id:`tth-${id}`, title:'Click to toggle raw/ISO'});
      const thOrigin = el('th', {text:'Origin'});
      const thPeer = el('th', {text:'Peer'});
      const thItems = el('th', {text:'Items'});
      thr.append(thId, thTime, thOrigin, thPeer, thItems);
      thead.appendChild(thr); table.appendChild(thead);
      const tbody = el('tbody');
      let timeIso = false;
      const fmt = ts=> timeIso ? new Date(ts).toISOString() : String(ts);
      thTime.onclick = ()=>{ timeIso = !timeIso; renderRows(); };
      const renderRows = ()=>{
        tbody.innerHTML='';
        ev.forEach(e => {
          const tr = el('tr');
          tr.append(
            el('td',{text:String(e.id)}),
            el('td',{text:fmt(e.ts)}),
            el('td',{text:String(e.origin)}),
            el('td',{text:e.peer_id}),
            el('td',{text:String(e.items_count)})
          );
          tbody.appendChild(tr);
        });
      };
      renderRows();
      table.appendChild(tbody);
      body.appendChild(table);
      // Basic column resize grips
      let startX=0, startW=0, target=null;
      table.querySelectorAll('th').forEach(th=>{
        th.style.position='relative';
        const grip = el('div',{style:'position:absolute;right:0;top:0;width:6px;height:100%;cursor:col-resize;'});
        th.appendChild(grip);
        grip.onmousedown = (e)=>{ startX=e.clientX; startW=th.offsetWidth; target=th; document.body.style.userSelect='none'; };
      });
      document.addEventListener('mousemove', e=>{ if(!target) return; const dx=e.clientX-startX; target.style.width=(startW+dx)+'px'; });
      document.addEventListener('mouseup', ()=>{ target=null; document.body.style.userSelect=''; });

    }
  }
  sel && (sel.onchange = render);
  render();

  // simple drag-reorder
  head.addEventListener('dragstart', e => { DRAG_ID = card.id; e.dataTransfer.effectAllowed='move'; });
}

async function init(){
  await refreshMode();
  await refreshStats();
  // default cards
  addCard('files');
  if (MODE === 'attached') addCard('stats');
  addCard('events', MODE==='attached' ? 'attached' : (ORDER[0]||null));

  document.getElementById('add-card').onclick = ()=> addCard('events');  // no separate file panel; files card handles its own actions

  // One-time board DnD handlers
  const board = document.getElementById('board');
  board.addEventListener('dragover', e => { e.preventDefault(); e.dataTransfer.dropEffect='move'; });
  board.addEventListener('drop', e => {
    e.preventDefault();
    if (!DRAG_ID) return;
    const src = document.getElementById(DRAG_ID);
    const targetCard = e.target.closest('.card');
    if (src && targetCard && src !== targetCard) {
      board.insertBefore(src, targetCard);
    } else if (src && !targetCard) {
      board.appendChild(src);
    }
    DRAG_ID = null;
  });
}
init();
