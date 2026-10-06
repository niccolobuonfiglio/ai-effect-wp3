const POLL_MS = 2500;
const KEY_DEBOUNCE_MS = 400;
const LOCKED_TEXT = 'API key required: enter the orchestrator API key above.';
const COLORS = { pending: '--pending', running: '--running', blocked: '--blocked',
                 completed: '--completed', failed: '--failed' };
let selected = null;
const expanded = new Set();      // task ids whose inputs and outputs are shown
let verification = null;         // last /verify result, for the selected workflow only

const $ = id => document.getElementById(id);
const css = name => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const time = iso => iso ? new Date(iso).toLocaleTimeString() : '';

class Unauthorized extends Error {}

function readKey() { try { return localStorage.getItem('orch_key') || ''; } catch { return ''; } }
function saveKey(v) { try { localStorage.setItem('orch_key', v); } catch { /* private mode */ } }

async function api(path, options = {}) {
  const key = $('apikey').value.trim();
  const headers = key ? { Authorization: 'Bearer ' + key } : {};
  const response = await fetch(path, { ...options, headers });
  if (response.status === 401) throw new Unauthorized(LOCKED_TEXT);
  if (!response.ok) {
    const detail = await response.json().then(b => b.detail, () => '');
    throw new Error(`${path}: HTTP ${response.status}${detail ? ' ' + detail : ''}`);
  }
  return response;
}

function say(text, isError = false) {
  $('message').textContent = text;
  $('message').className = isError ? 'small error' : 'small';
}

async function checkOnline() {
  try { await fetch('/health'); $('online').textContent = 'online'; $('online').style.background = css('--completed'); }
  catch { $('online').textContent = 'offline'; $('online').style.background = css('--failed'); }
}

async function loadWorkflows() {
  const data = await (await api('/workflows')).json();
  if (!data.workflows.length) { $('workflows').innerHTML = '<li class="empty">No workflows yet.</li>'; return; }
  if (!selected) selected = data.workflows[0].workflow_id;
  $('workflows').innerHTML = data.workflows.map(w => `
    <li data-id="${esc(w.workflow_id)}" class="${w.workflow_id === selected ? 'sel' : ''}">
      <span><span class="mono">${esc(w.workflow_id)}</span><br><span class="small">${esc(w.name || '')}  ${time(w.created_at)}</span></span>
      <span class="st st-${esc(w.status)}">${esc(w.status)}</span>
    </li>`).join('');
  $('workflows').querySelectorAll('li[data-id]').forEach(li =>
    li.onclick = () => { selected = li.dataset.id; tick(); });
}

async function loadServices() {
  const data = await (await api('/orchestrator/health/services')).json();
  if (!data.services.length) { $('services').innerHTML = '<div class="empty">No services registered yet.</div>'; return; }
  $('services').innerHTML = `
    <div class="small">overall <span class="st st-${esc(data.overall_status)}">${esc(data.overall_status)}</span></div>
    <table><tr><th>Service</th><th>Status</th><th></th></tr>
    ${data.services.map(s => `
      <tr title="${esc(s.last_error || '')}">
        <td class="mono">${esc(s.service_name)}<br><span class="small">${esc(s.circuit_state)}${s.failure_count ? '  ' + s.failure_count + ' failures' : ''}${s.retry_after ? '  next probe ' + time(s.retry_after) : ''}</span></td>
        <td class="st st-${esc(s.status)}">${esc(s.status)}</td>
        <td>${s.status !== 'HEALTHY' ? `<button data-recover="${esc(s.service_name)}">Recover</button>` : ''}</td>
      </tr>`).join('')}
    </table>`;
  $('services').querySelectorAll('button[data-recover]').forEach(b => b.onclick = async () => {
    try {
      const r = await (await api(`/orchestrator/health/services/${encodeURIComponent(b.dataset.recover)}/recover`, { method: 'POST' })).json();
      say(`${r.service_name}: ${r.message}`);
      tick();
    } catch (e) { say(e.message, true); }
  });
}

function drawGraph(graph) {
  // Layered layout: a node's column is its longest distance from a start node.
  const incoming = {}; graph.nodes.forEach(n => incoming[n.key] = []);
  graph.edges.forEach(e => { if (incoming[e.target]) incoming[e.target].push(e.source); });
  const depth = {};
  const depthOf = (k, seen = new Set()) => {
    if (k in depth) return depth[k];
    if (seen.has(k)) return 0;
    seen.add(k);
    depth[k] = incoming[k].length ? 1 + Math.max(...incoming[k].map(p => depthOf(p, seen))) : 0;
    return depth[k];
  };
  graph.nodes.forEach(n => depthOf(n.key));
  const columns = {};
  graph.nodes.forEach(n => (columns[depth[n.key]] ||= []).push(n));

  const W = 170, H = 46, GX = 60, GY = 22;
  const pos = {};
  Object.entries(columns).forEach(([c, nodes]) => nodes.forEach((n, i) =>
    pos[n.key] = { x: 10 + c * (W + GX), y: 10 + i * (H + GY) }));
  const width = 20 + Object.keys(columns).length * (W + GX);
  const height = 20 + Math.max(...Object.values(columns).map(c => c.length)) * (H + GY);

  const edges = graph.edges.filter(e => pos[e.source] && pos[e.target]).map(e => {
    const a = pos[e.source], b = pos[e.target];
    return `<line x1="${a.x + W}" y1="${a.y + H / 2}" x2="${b.x}" y2="${b.y + H / 2}" stroke="${css('--muted')}" stroke-width="1.5" marker-end="url(#arrow)"/>`;
  }).join('');
  const nodes = graph.nodes.map(n => {
    const p = pos[n.key], color = css(COLORS[n.status] || '--pending');
    const health = n.service_status ? `service ${n.service_status} (${n.circuit_state})` : 'service health unknown';
    return `<g><title>${esc(n.key)}\n${esc(n.status)}\n${esc(health)}</title>
      <rect x="${p.x}" y="${p.y}" width="${W}" height="${H}" rx="8" fill="${color}" fill-opacity="0.15" stroke="${color}" stroke-width="2"/>
      <text x="${p.x + 10}" y="${p.y + 19}">${esc(n.container_name)}</text>
      <text x="${p.x + 10}" y="${p.y + 36}" fill="${color}" style="fill:${color};font-weight:600">${esc(n.status)}${n.service_status && n.service_status !== 'HEALTHY' ? '  ' + esc(n.service_status) : ''}</text></g>`;
  }).join('');
  $('graph').innerHTML = `<svg width="${width}" height="${height}" role="img" aria-label="workflow graph">
    <defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">
      <path d="M0,0 L10,5 L0,10 z" fill="${css('--muted')}"/></marker></defs>${edges}${nodes}</svg>`;
}

function historyText(h) {
  const parts = [];
  if (h.claims > 1) parts.push(`${h.claims} claims`);
  for (const k of ['reclaims', 'deferrals', 'retries']) if (h[k]) parts.push(`${h[k]} ${h[k] === 1 ? k.slice(0, -1).replace(/ie$/, 'y') : k}`);
  return parts.join('  ');
}

// POST an operator action, report the API's message and refresh.
async function act(path) {
  try {
    const r = await (await api(path, { method: 'POST' })).json();
    say(r.message);
    tick();
  } catch (e) { say(e.message, true); }
}

// One data reference: format, size, checksum and where it lives. Inline data
// travels base64-encoded in the URI itself, so only its start is shown.
function refText(r) {
  const uri = r.uri.length > 90 ? r.uri.slice(0, 90) + '…' : r.uri;
  const size = r.size_bytes != null ? `  ${r.size_bytes} B` : '';
  return `<div><span class="small">${esc(r.protocol)}  ${esc(r.format)}${size}</span>
    ${r.checksum ? `<span class="mono small">  ${esc(r.checksum)}</span>` : ''}<br><span class="mono small">${esc(uri)}</span></div>`;
}

function refsRow(t) {
  const list = refs => refs.length ? refs.map(refText).join('') : '<div class="small">none</div>';
  return `<tr class="refs"><td colspan="6"><div class="refs-grid">
    <div><div class="small st">Inputs</div>${list(t.input_refs || [])}</div>
    <div><div class="small st">Outputs</div>${list(t.output_refs || [])}</div></div></td></tr>`;
}

// Verification is run on demand: it reads the whole workflow at one moment.
async function verify() {
  const wf = selected;
  try {
    verification = await (await api(`/workflows/${encodeURIComponent(wf)}/verify`)).json();
    if (wf === selected) showVerification();
  } catch (e) { say(e.message, true); }
}

function showVerification() {
  const button = '<button id="verify-btn">Verify</button>';
  if (!selected) { $('verify').innerHTML = '<div class="empty">Select a workflow.</div>'; return; }
  if (!verification || verification.workflow_id !== selected) {
    $('verify').innerHTML = `<div class="empty">Check this workflow's state against the engine's properties. ${button}</div>`;
  } else {
    const v = verification;
    $('verify').innerHTML = `
      <div><span class="pill" style="background:${css(v.verified ? '--completed' : '--failed')}">${v.verified ? 'verified' : 'problems found'}</span>
        <span class="small">checked ${time(v.checked_at)} while ${esc(v.status)}</span> ${button}</div>
      <table>${v.checks.map(c => `<tr>
        <td class="st ${c.passed ? 'st-completed' : 'st-failed'}">${c.passed ? 'pass' : 'fail'}</td>
        <td class="mono">${esc(c.name)}</td>
        <td class="small">${c.problems.map(esc).join('<br>')}</td></tr>`).join('')}</table>`;
  }
  $('verify-btn').onclick = verify;
}

async function loadSelected() {
  if (!selected) return;
  const [graph, tasks, logs] = await Promise.all([
    api(`/workflows/${selected}/graph`).then(r => r.json()),
    api(`/workflows/${selected}/tasks`).then(r => r.json()),
    api(`/workflows/${selected}/logs?lines=150`).then(r => r.text()),
  ]);
  const wf = selected, h = tasks.history || {};
  const badge = h.clean === false
    ? `<span class="pill" style="background:${css('--blocked')}" title="a task was claimed more than once">${esc(historyText({ ...h, claims: 1 }) || 'claimed again')}</span>`
    : `<span class="pill" style="background:${css('--completed')}" title="every task claimed once">clean</span>`;
  $('wf-title').innerHTML = `${esc(graph.name || 'Workflow')} <span class="mono small">${esc(wf)}</span>  <span class="st st-${esc(graph.status)}">${esc(graph.status)}</span> ${badge}`
    + (graph.status === 'failed' ? ` <button id="resume">Resume</button>` : '');
  if ($('resume')) $('resume').onclick = () => act(`/workflows/${encodeURIComponent(wf)}/resume`);
  drawGraph(graph);
  $('tasks').innerHTML = `<table><tr><th>Task</th><th>Status</th><th>History</th><th>Updated</th><th>Error</th><th></th></tr>
    ${tasks.tasks.map(t => `<tr><td class="mono toggle" data-toggle="${esc(t.task_id)}" title="inputs and outputs">${expanded.has(t.task_id) ? '▾' : '▸'}&nbsp;${esc(t.node_key)}</td>
      <td class="st st-${esc(t.status)}">${esc(t.status)}</td>
      <td class="small">${esc(historyText(t.history || {}))}</td>
      <td class="small">${time(t.updated_at)}</td>
      <td class="small">${esc(t.error || '')}</td>
      <td>${t.status === 'failed' ? `<button data-retry="${esc(t.task_id)}">Retry</button>` : ''}</td></tr>
      ${expanded.has(t.task_id) ? refsRow(t) : ''}`).join('')}</table>`;
  $('tasks').querySelectorAll('[data-toggle]').forEach(td => td.onclick = () => {
    const id = td.dataset.toggle;
    expanded.has(id) ? expanded.delete(id) : expanded.add(id);
    loadSelected().catch(e => say(e.message, true));
  });
  $('tasks').querySelectorAll('button[data-retry]').forEach(b =>
    b.onclick = () => act(`/workflows/${encodeURIComponent(wf)}/tasks/${encodeURIComponent(b.dataset.retry)}/retry`));
  showVerification();
  const pre = $('logs'), atBottom = pre.scrollTop + pre.clientHeight >= pre.scrollHeight - 4;
  pre.textContent = logs || '(no log lines)';
  if (atBottom) pre.scrollTop = pre.scrollHeight;
}

async function loadAlerts() {
  const data = await (await api('/orchestrator/alerts?limit=15')).json();
  if (!data.alerts.length) { $('alerts').innerHTML = '<div class="empty">No alerts.</div>'; return; }
  const cls = { SERVICE_DOWN: 'st-failed', SERVICE_RECOVERED: 'st-completed', SERVICE_MANUAL_RECOVERY: 'st-blocked', WORKFLOW_VERIFICATION_FAILED: 'st-failed', WORKFLOW_STALLED: 'st-failed' };
  $('alerts').innerHTML = `<table>${data.alerts.map(a => `<tr${a.workflow_id ? ` class="link" data-wf="${esc(a.workflow_id)}" title="open this workflow"` : ''}>
    <td class="small">${time(a.timestamp)}</td>
    <td class="st ${cls[a.alert] || ''}">${esc(a.alert || '')}</td>
    <td class="mono">${esc(a.service_name || a.workflow_id || '')}</td>
    <td class="small">${esc(a.last_error || a.summary || '')}</td></tr>`).join('')}</table>`;
  $('alerts').querySelectorAll('tr[data-wf]').forEach(tr =>
    tr.onclick = () => { selected = tr.dataset.wf; tick(); });
}

// Without a valid key no panel can load
function showLocked() {
  $('workflows').innerHTML = `<li class="empty error">${LOCKED_TEXT}</li>`;
  for (const id of ['services', 'alerts', 'tasks', 'verify']) $(id).innerHTML = `<div class="empty error">${LOCKED_TEXT}</div>`;
  $('graph').innerHTML = '';
  $('wf-title').textContent = 'Locked';
  $('logs').textContent = LOCKED_TEXT;
}

async function tick() {
  checkOnline();
  try {
    await Promise.all([loadWorkflows(), loadServices(), loadAlerts()]);
    await loadSelected();
    // A successful refresh clears an earlier error; a Recover confirmation stays.
    if ($('message').classList.contains('error')) say('');
  } catch (e) {
    if (e instanceof Unauthorized) showLocked();
    say(e.message, true);
  }
}

function applyKey() { saveKey($('apikey').value.trim()); tick(); }
let keyTimer = null;
$('apikey').addEventListener('input', () => {
  clearTimeout(keyTimer);
  keyTimer = setTimeout(applyKey, KEY_DEBOUNCE_MS);
});
$('apikey').addEventListener('keydown', e => {
  if (e.key === 'Enter') { clearTimeout(keyTimer); applyKey(); }
});

$('apikey').value = readKey();
tick();
setInterval(tick, POLL_MS);
