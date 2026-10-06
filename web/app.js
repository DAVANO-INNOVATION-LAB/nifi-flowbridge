'use strict';
const $ = id => document.getElementById(id);
const MAX_BYTES = 2 * 1024 * 1024;
const descriptions = {
  "continuous-worker": "Export an executable polling worker for the strict three-lane NiFi S3 profile. Controlled stop/drain and byte reconciliation are required before cutover; new arrivals remain in source storage.",
  airflow: "Generate a paused Airflow DAG for explicitly supported one-shot HTTP and transformation workflows. Streaming semantics and unmapped processors block export.",
  "nifi-upgrade": "Prepare a review-only NiFi 1 to 2 rule plan. This does not produce a validated NiFi 2 flow or change your source.",
  seatunnel: 'Generate a SeaTunnel configuration for the supported flow, with deployment notes and a compatibility report.',
  'camel-k': 'Generate a Camel K integration for the supported flow. Review configuration before deploying to Kubernetes.',
  kafka: 'Generate a Kafka Streams project and an importable Flowbridge manifest. Kafka is the destination runtime, not a universal flow format.',
  nifi: 'Generate a NiFi flow definition for the supported subset. Review processors and connection settings before importing.',
  flowbridge: 'Export the normalized flow as an open, portable JSON document. Use it to inspect or re-import the supported flow.'
};
let preparedRequest = null;
let busy = false;
function status(message, error = false) { $('status').textContent = message; $('status').classList.toggle('error', error); }
function setBusy(value) { busy = value; for (const id of ['analyze-button', 'convert-button', 'example-button', 'http-example-button', 'media-example-button', 'continuous-example-button', 'clear-button']) $(id).disabled = value; $('download-button').disabled = value || !$('review-ack').checked || !preparedRequest; $('review-ack').disabled = value; $('source').disabled = value; $('target').disabled = value; $('document').readOnly = value; $('file-input').disabled = value; platformControls(); }
function invalidate() { preparedRequest = null; $('review-ack').checked = false; $('download-button').disabled = true; $('artifacts').hidden = true; $('report').hidden = true; $('graph-assessment').hidden = true; status($('document').value.trim() ? 'Flow added. Check the source or prepare an export.' : 'Add a flow to begin.'); }
function requestBody() {
  const raw = $('document').value.trim();
  if (!raw) throw new Error('Add a JSON flow document first.');
  if (new TextEncoder().encode(raw).length > MAX_BYTES) throw new Error('This document exceeds the 2 MB limit.');
  let document; try { document = raw.startsWith('<') ? raw : JSON.parse(raw); } catch { throw new Error('This is not valid JSON or NiFi XML. Check the document and try again.'); }
  if (!document || (typeof document !== 'object' && typeof document !== 'string') || Array.isArray(document)) throw new Error('The flow document must be a JSON object.');
  return { source: $('source').value, target: $('target').value, document, nifi_version: $('nifi-version').value, allow_partial: $('partial-ack').checked, batch_contract: $('batch-contract').checked ? {mode:'one_shot',acknowledge_scheduling_change:true} : null };
}
async function jsonRequest(path, body) {
  const response = await fetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  const data = await response.json().catch(() => null);
  if (!response.ok && body.allow_partial && data?.files && Object.keys(data.files).length) return data;
  if (!response.ok) { if (data?.report) showReport(data.report, path.endsWith('/analyze') ? 'source' : 'export'); throw new Error(errorMessage(data?.error) || (data?.report?.errors?.length ? 'Resolve the compatibility blockers shown below.' : `The request failed (${response.status}).`)); }
  if (!data) throw new Error('The local server returned an unreadable response.');
  return data;
}
function errorMessage(value) { return typeof value === 'string' ? value : value?.message; }
function findingText(item) { if (typeof item === 'string') return item; return item?.message || item?.detail || item?.description || JSON.stringify(item); }
function showReport(report = {}, mode = 'export') {
  const errors = Array.isArray(report.errors) ? report.errors : [];
  const warnings = Array.isArray(report.warnings) ? report.warnings : [];
  const container = $('report-findings'); container.replaceChildren();
  $('report').hidden = false;
  $('report-summary').textContent = `${errors.length} blocker${errors.length === 1 ? '' : 's'} · ${warnings.length} warning${warnings.length === 1 ? '' : 's'}`;
  for (const [kind, entries] of [['error', errors], ['warning', warnings]]) for (const item of entries) {
    const div = document.createElement('div'); div.className = `finding ${kind}`;
    const title = document.createElement('strong'); title.textContent = kind === 'error' ? 'Export blocker' : 'Review required';
    const detail = document.createElement('span'); detail.textContent = findingText(item);
    div.append(title, detail); container.append(div);
  }
  if (!errors.length && !warnings.length) { const div = document.createElement('div'); div.className = 'finding success'; div.textContent = mode === 'source' ? 'Source accepted. Prepare an export to check compatibility with your selected destination.' : 'No compatibility findings for the supported mapping. Review and test the generated project before deployment.'; container.append(div); }
  return errors.length;
}
function showFiles(files) {
  const entries = Object.entries(files || {}).filter(([, content]) => typeof content === 'string');
  if (!entries.length) return false;
  const list = $('file-list'); list.replaceChildren();
  function select(name, content, button) { for (const sibling of list.children) { sibling.classList.remove('selected'); sibling.setAttribute('aria-current', 'false'); } button.classList.add('selected'); button.setAttribute('aria-current', 'true'); $('preview-name').textContent = name; $('preview-code').textContent = content; }
  entries.forEach(([name, content], index) => { const button = document.createElement('button'); button.type = 'button'; button.className = 'file-button'; button.textContent = name; button.addEventListener('click', () => select(name, content, button)); list.append(button); if (!index) select(name, content, button); });
  $('artifacts').hidden = false; return true;
}
async function run(mode) {
  if (busy) return;
  invalidate(); setBusy(true);
  try {
    const body = requestBody(); status(mode === 'analyze' ? 'Checking the source…' : 'Checking compatibility and preparing your project…');
    const data = await jsonRequest(`/api/${mode === 'analyze' ? 'assess' : mode}`, body);
    if (data.assessment) showGraph(data);
    const errors = showReport(data.report || data, mode === 'analyze' ? 'source' : 'export');
    if (errors) { if (body.allow_partial && showFiles(data.files)) { preparedRequest = body; status('Incomplete review package prepared. Blockers remain; this is not a successful migration.', true); } else status('Resolve the compatibility blockers before exporting.', true); return; }
    if (mode === 'convert') { if (!showFiles(data.files)) throw new Error('No project files were generated. Review the report.'); preparedRequest = body; status('Export prepared. Review the findings and generated files below.'); }
    else status('Assessment complete. Review the inventory, then prepare an export to check executable mapping coverage.');
  } catch (error) { status(error.message || 'The request could not be completed.', true); }
  finally { setBusy(false); }
}
async function loadFile(file) {
  if (busy || !file) return;
  invalidate();
  if (file.size > MAX_BYTES) { status('Choose a JSON file smaller than 2 MB.', true); return; }
  try {
    if (file.name.toLowerCase().endsWith('.zip')) {
      const response = await fetch('/api/import-package', {method:'POST',headers:{'Content-Type':'application/zip'},body:await file.arrayBuffer()});
      const data = await response.json();
      if (!response.ok || !data.blueprint) throw new Error(data.error || 'This media package could not be imported intact.');
      $('document').value = JSON.stringify(data.blueprint,null,2); $('source').value='auto'; $('nifi-version').value='auto';
      $('file-detail').textContent = file.name + ' · verified manifest import'; invalidate(); status('Media package imported back to its blueprint. Native runtime execution is a separate check.'); return;
    }
    $('document').value = await file.text(); $('file-detail').textContent = `${file.name} · ${(file.size / 1024).toFixed(1)} KB`; invalidate(); requestBody(); }
  catch (error) { status(error.message || 'The file could not be read.', true); }
}
$('file-input').addEventListener('change', event => loadFile(event.target.files[0]));
$('document').addEventListener('input', invalidate);
$('source').addEventListener('change', invalidate);
$('nifi-version').addEventListener('change', invalidate);
$('batch-contract').addEventListener('change', invalidate);
$('partial-ack').addEventListener('change', invalidate);
$('target').addEventListener('change', () => { $('target-description').textContent = descriptions[$('target').value]; $('batch-contract-label').hidden = $('target').value !== 'airflow'; invalidate(); });
$('clear-button').addEventListener('click', () => { $('document').value = ''; $('file-input').value = ''; $('file-detail').textContent = 'Up to 2 MB · processed on this local server'; invalidate(); $('document').focus(); });
$('analyze-button').addEventListener('click', () => run('analyze'));
$('convert-button').addEventListener('click', () => run('convert'));
$('review-ack').addEventListener('change', () => { $('download-button').disabled = busy || !$('review-ack').checked || !preparedRequest; });
$('example-button').addEventListener('click', async () => { if (busy) return; setBusy(true); try { const response = await fetch('/api/example'); if (!response.ok) throw new Error('The example could not be loaded.'); const data = await response.json(); $('document').value = JSON.stringify(data.document || data, null, 2); $('source').value = typeof data.source === 'string' ? data.source : 'auto'; $('target').value = 'seatunnel'; $('target-description').textContent = descriptions.seatunnel; $('batch-contract-label').hidden = true; $('file-detail').textContent = 'Example loaded · no credentials included'; invalidate(); } catch (error) { status(error.message, true); } finally { setBusy(false); } });
$('download-button').addEventListener('click', async () => {
  if (busy || !preparedRequest || !$('review-ack').checked) return;
  setBusy(true); status('Packaging your project…');
  try {
    const response = await fetch('/api/download', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(preparedRequest) });
    if (!response.ok) { const data = await response.json().catch(() => ({})); if (data.report) showReport(data.report); throw new Error(errorMessage(data.error) || 'The project could not be downloaded.'); }
    if (!(response.headers.get('content-type') || '').includes('application/zip')) throw new Error('The server did not return a project archive.');
    const url = URL.createObjectURL(await response.blob()); const link = document.createElement('a'); link.href = url; link.download = `flowbridge-${preparedRequest.target}.zip`; document.body.append(link); link.click(); link.remove(); setTimeout(() => URL.revokeObjectURL(url), 10000); status('Project downloaded. Review its files and test before deployment.');
  } catch (error) { status(error.message, true); } finally { setBusy(false); }
});
for (const eventName of ['dragenter', 'dragover']) $('drop-zone').addEventListener(eventName, event => { event.preventDefault(); if (!busy) $('drop-zone').classList.add('dragging'); });
for (const eventName of ['dragleave', 'drop']) $('drop-zone').addEventListener(eventName, event => { event.preventDefault(); $('drop-zone').classList.remove('dragging'); });
$('drop-zone').addEventListener('drop', event => { if (event.dataTransfer.files.length !== 1) { status('Choose one JSON flow at a time.', true); return; } loadFile(event.dataTransfer.files[0]); });
$('target-description').textContent = descriptions[$('target').value];

// Connected operations are deliberately separate from file conversion.
// Credentials remain in these form controls and server memory; never browser storage.
let liveEnabled = false;
let liveBusy = false;
let livePlan = null;
let liveJob = null;
let liveState = '';
let liveTimer = null;
const liveTerminal = new Set(['completed', 'failed', 'cancelled', 'canceled', 'interrupted']);
function liveStatus(message, error = false) { $('live-status').textContent = message; $('live-status').classList.toggle('error', error); }
function liveControls() {
  const active = liveJob && !liveTerminal.has(liveState);
  $('live-assess').disabled = !liveEnabled || liveBusy || active;
  $('live-start').disabled = !liveEnabled || liveBusy || !livePlan || !$('live-start-ack').checked || active;
  $('live-refresh').disabled = liveBusy;
  $('live-find').disabled = !liveEnabled || liveBusy;
  $('live-cancel').disabled = liveBusy || !active;
  $('live-cutover-button').disabled = liveBusy || !active || !['mirroring', 'ready'].includes(liveState) || !['live-producers-stopped', 'live-consumers-stopped', 'live-cutover-ack'].every(id => $(id).checked);
  for (const control of $('live-form').querySelectorAll('input,select')) control.disabled = liveBusy || (!!active && control.id !== 'live-token');
}
function invalidateLivePlan() {
  livePlan = null; $('live-assessment').hidden = true; $('live-start-ack').checked = false;
  if (!liveJob || liveTerminal.has(liveState)) liveStatus(liveEnabled ? 'Enter connections and assess them before starting a transfer.' : 'Connected migration is not enabled on this server.');
  liveControls();
}
function clearLiveSecrets() { for (const id of ['live-source-password', 'live-target-password']) $(id).value = ''; }
function liveConnection(side) {
  const prefix = `live-${side}-`;
  const value = {brokers: $(prefix + 'brokers').value.trim(), topic: $(prefix + 'topic').value.trim(), security_protocol: $(prefix + 'security').value};
  if (value.security_protocol === 'SASL_SSL') { value.username = $(prefix + 'username').value; value.password = $(prefix + 'password').value; value.sasl_mechanism = $(prefix + 'mechanism').value; }
  return value;
}
async function liveRequest(path, payload) {
  const token = $('live-token').value.trim();
  if (!token) throw new Error('Enter the owner access token to use connected migration.');
  const options = {method: payload === undefined ? 'GET' : 'POST', headers: {Authorization: `Bearer ${token}`}, cache: 'no-store'};
  if (payload !== undefined) { options.headers['Content-Type'] = 'application/json'; options.body = JSON.stringify(payload); }
  const response = await fetch(path, options);
  const data = await response.json().catch(() => null);
  if (!response.ok) { if (data?.report && !path.includes('/platform/')) showLiveReport(data.report); throw new Error(errorMessage(data?.error) || (response.status === 401 || response.status === 403 ? 'The owner token was not accepted.' : `Connected migration request failed (${response.status}).`)); }
  if (!data) throw new Error('The server returned an unreadable response.');
  return data;
}
function safeLiveSummary(value) {
  if (Array.isArray(value)) return value.map(safeLiveSummary);
  if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, /password|secret|token|credential|authorization/i.test(key) ? '[redacted]' : safeLiveSummary(item)]));
  return value;
}
function showLiveReport(report = {}) {
  $('live-assessment').hidden = false; $('live-summary').textContent = ''; const findings = $('live-findings'); findings.replaceChildren();
  for (const kind of ['errors', 'warnings']) for (const item of Array.isArray(report[kind]) ? report[kind] : []) { const row = document.createElement('div'); row.className = `finding ${kind === 'errors' ? 'error' : 'warning'}`; row.textContent = findingText(item); findings.append(row); }
}
async function refreshLiveAvailability() {
  try {
    const response = await fetch('/api/live/status', {cache:'no-store'});
    if (!response.ok) throw new Error('Connected migration is unavailable on this server.');
    const data = await response.json(); liveEnabled = data.enabled === true;
    $('live-availability').textContent = liveEnabled ? 'AVAILABLE · OWNER ACCESS' : 'DISABLED ON SERVER';
    if (!liveJob) liveStatus(liveEnabled ? 'Enter connections and assess them before starting a transfer.' : 'Connected migration is disabled. Enable it on the server with an owner access token to use this workspace.');
  } catch (error) { liveEnabled = false; $('live-availability').textContent = 'UNAVAILABLE'; if (!liveJob) liveStatus(error.message, true); }
  liveControls(); platformControls();
}
function renderLiveJob(data) {
  liveState = typeof data.state === 'string' ? data.state : 'unknown'; $('live-job').hidden = false; $('live-job-state').textContent = liveState.replaceAll('_', ' ');
  $('live-copied').textContent = Number.isFinite(data.copied) ? data.copied.toLocaleString() : 'Not reported';
  $('live-lag').textContent = Number.isFinite(data.lag) ? data.lag.toLocaleString() : 'Not reported';
  $('live-ready').textContent = data.cutover_ready === true ? 'Ready for review' : data.cutover_ready === false ? 'Not ready' : 'Not reported';
  $('live-job-error').hidden = !data.error; $('live-job-error').textContent = errorMessage(data.error) || (data.error ? 'The transfer reported an error.' : '');
  $('live-job-result').hidden = !data.result;
  $('live-job-result-json').textContent = data.result ? JSON.stringify(safeLiveSummary(data.result), null, 2) : '';
  const events = $('live-events'); events.replaceChildren();
  for (const event of (Array.isArray(data.events) ? data.events.slice(-12) : [])) { const item = document.createElement('li'); item.textContent = findingText(event); events.append(item); }
  if (liveState === 'completed') liveStatus('The server reports cutover completed. Review the result before directing your applications to the destination.');
  else if (liveTerminal.has(liveState)) liveStatus(`The server reports this transfer is ${liveState}. Records already copied remain at the destination.`, liveState === 'failed');
  else liveStatus(`Server transfer state: ${liveState.replaceAll('_', ' ')}. Monitor progress here before requesting cutover.`);
  liveControls();
}
function scheduleLivePoll() { clearTimeout(liveTimer); if (liveJob && !liveTerminal.has(liveState)) liveTimer = setTimeout(pollLiveJob, 3000); }
async function pollLiveJob() {
  if (!liveJob || liveTerminal.has(liveState)) return;
  if (liveBusy) { scheduleLivePoll(); return; }
  try { renderLiveJob(await liveRequest(`/api/live/jobs/${encodeURIComponent(liveJob)}`)); }
  catch (error) { liveStatus(`Could not refresh progress: ${error.message} The transfer may still be running.`, true); }
  scheduleLivePoll();
}
$('live-form').addEventListener('submit', async event => {
  event.preventDefault(); if (liveBusy || !liveEnabled) return;
  invalidateLivePlan(); liveBusy = true; liveControls(); liveStatus('Assessing source and destination connections…');
  try {
    const result = await liveRequest('/api/live/assess', {name: $('live-name').value.trim(), source: liveConnection('source'), target: liveConnection('target'), groups: $('live-groups').value.split(',').map(value => value.trim()).filter(Boolean)});
    showLiveReport(result.report); $('live-summary').textContent = result.summary ? (typeof result.summary === 'string' ? result.summary : JSON.stringify(safeLiveSummary(result.summary), null, 2)) : '';
    if (result.report?.ok === true && result.plan_id) { livePlan = result.plan_id; clearLiveSecrets(); liveStatus('Assessment complete. Review its findings and authorize the data transfer when ready.'); }
    else liveStatus('Assessment did not approve a transfer. Resolve the reported blockers.', true);
  } catch (error) { liveStatus(error.message, true); }
  finally { liveBusy = false; liveControls(); }
});
$('live-start').addEventListener('click', async () => {
  if (liveBusy || !livePlan || !$('live-start-ack').checked) return;
  liveBusy = true; liveControls(); liveStatus('Requesting data transfer…');
  try {
    const result = await liveRequest('/api/live/start', {plan_id: livePlan, acknowledge: true});
    if (!result.job_id) throw new Error('The server did not return a transfer identifier. Verify server status before retrying.');
    liveJob = result.job_id; livePlan = null; $('live-start-ack').checked = false; clearLiveSecrets();
    for (const id of ['live-producers-stopped', 'live-consumers-stopped', 'live-cutover-ack']) $(id).checked = false;
    renderLiveJob(result); scheduleLivePoll();
  } catch (error) { liveStatus(`${error.message} If the request was interrupted, verify server status before starting another transfer.`, true); }
  finally { liveBusy = false; liveControls(); }
});
$('live-cancel').addEventListener('click', async () => {
  if (liveBusy || !liveJob || liveTerminal.has(liveState)) return;
  liveBusy = true; liveControls();
  try { const result = await liveRequest('/api/live/cancel', {job_id: liveJob}); renderLiveJob(result); scheduleLivePoll(); }
  catch (error) { liveStatus(`Cancellation was not confirmed: ${error.message}`, true); }
  finally { liveBusy = false; liveControls(); }
});
$('live-cutover-button').addEventListener('click', async () => {
  if ($('live-cutover-button').disabled || !liveJob) return;
  liveBusy = true; liveControls(); liveStatus('Requesting final cutover checks. Keep producers and consumers stopped.');
  try { const result = await liveRequest('/api/live/cutover', {job_id: liveJob, producers_stopped:true, consumers_stopped:true, acknowledge:true}); renderLiveJob(result); scheduleLivePoll(); }
  catch (error) { liveStatus(`Cutover was not confirmed: ${error.message} Keep applications stopped while checking the transfer state.`, true); scheduleLivePoll(); }
  finally { liveBusy = false; liveControls(); }
});
for (const id of ['live-start-ack', 'live-producers-stopped', 'live-consumers-stopped', 'live-cutover-ack']) $(id).addEventListener('change', liveControls);
for (const control of $('live-form').querySelectorAll('input,select')) control.addEventListener('input', () => { if (control.id !== 'live-token') invalidateLivePlan(); });
for (const side of ['source','target']) $(`live-${side}-security`).addEventListener('change', () => { $(`live-${side}-auth`).hidden = $(`live-${side}-security`).value !== 'SASL_SSL'; invalidateLivePlan(); });
$('live-find').addEventListener('click', async () => {
  if (liveBusy || !liveEnabled) return;
  liveBusy = true; liveControls(); liveStatus('Finding existing transfers…');
  try {
    const response = await liveRequest('/api/live/jobs');
    const jobs = Array.isArray(response.jobs) ? response.jobs : [];
    // The server returns newest first; prefer an active job, then the latest finished job.
    const job = jobs.find(item => item && !liveTerminal.has(item.state)) || jobs[0];
    if (!job) { liveStatus('No existing transfers were found. No new transfer was started.'); return; }
    const id = job.job_id || job.id;
    if (typeof id !== 'string' || !id) throw new Error('The server did not return a valid transfer identifier.');
    clearTimeout(liveTimer); liveJob = id; livePlan = null; $('live-assessment').hidden = true; $('live-start-ack').checked = false;
    for (const control of ['live-producers-stopped', 'live-consumers-stopped', 'live-cutover-ack']) $(control).checked = false;
    renderLiveJob(job); scheduleLivePoll();
  } catch (error) { liveStatus(`Could not find existing transfers: ${error.message}`, true); }
  finally { liveBusy = false; liveControls(); }
});
$('live-refresh').addEventListener('click', refreshLiveAvailability);
window.addEventListener('pagehide', () => { clearTimeout(liveTimer); clearLiveSecrets(); $('live-token').value = ''; });
refreshLiveAvailability();

let platformBusy = false;
function platformStatus(message, error = false) { $('platform-status').textContent = message; $('platform-status').classList.toggle('error', error); }
function platformControls() {
  $('platform-inspect').disabled = platformBusy || !liveEnabled;
  $('platform-export').disabled = platformBusy || !liveEnabled || $('platform-kind').value === 'seatunnel' || busy;
  for (const control of $('platform-form').querySelectorAll('input,select')) control.disabled = platformBusy;
}
function platformPayload() {
  const raw = $('platform-url').value.trim(); let url;
  try { url = new URL(raw); } catch { throw new Error('Enter a valid HTTPS API base URL.'); }
  const allowLocal = $('platform-loopback').checked;
  if (url.username || url.password || url.search || url.hash) throw new Error('Use an API URL without embedded credentials, query parameters, or fragments.');
  if (url.protocol !== 'https:' && !(allowLocal && url.protocol === 'http:' && ['127.0.0.1', '[::1]'].includes(url.hostname))) throw new Error('HTTPS is required. For a local test, explicitly allow HTTP and use 127.0.0.1 or [::1].');
  const platform = $('platform-kind').value;
  const payload = {platform, url: raw, bearer_token: $('platform-bearer').value, allow_http_loopback: allowLocal};
  if (platform === 'nifi') { payload.group_id = $('platform-group').value.trim(); if (!payload.group_id) throw new Error('Enter the NiFi process group ID.'); }
  if (platform === 'camel-k') { payload.namespace = $('platform-namespace').value.trim(); payload.name = $('platform-integration').value.trim(); if (!payload.namespace || !payload.name) throw new Error('Enter the namespace and Camel K integration name.'); }
  if (platform === 'seatunnel') { payload.job_id = $('platform-job').value.trim(); if (!payload.job_id) throw new Error('Enter the SeaTunnel job ID.'); }
  return payload;
}
async function platformOperation(operation) {
  if (platformBusy || !liveEnabled || (operation === 'export' && busy)) return;
  if (operation === 'export' && $('platform-kind').value === 'seatunnel') { platformStatus('SeaTunnel flow export is not supported in this release. Status inspection is available.', true); return; }
  $('platform-result').hidden = true;
  try {
    const payload = platformPayload(); platformBusy = true; platformControls(); platformStatus(operation === 'inspect' ? 'Inspecting the running resource…' : 'Fetching its flow definition…');
    const result = await liveRequest(`/api/live/platform/${operation}`, payload);
    $('platform-bearer').value = '';
    if (operation === 'export') {
      if (!result.document || typeof result.document !== 'object' || !['nifi', 'camel-k'].includes(result.source)) throw new Error('The platform did not return a supported flow definition.');
      const document = JSON.stringify(result.document, null, 2);
      if (new TextEncoder().encode(document).length > MAX_BYTES) throw new Error('The fetched flow exceeds the converter’s 2 MB limit.');
      if (busy) throw new Error('The file converter is busy. Wait for its current request to finish, then fetch again.');
      $('document').value = document; $('source').value = result.source; $('file-input').value = ''; $('file-detail').textContent = `Fetched from running ${result.source === 'nifi' ? 'NiFi' : 'Camel K'} · review before conversion`; invalidate();
      platformStatus('Flow loaded into the converter above. Check compatibility before preparing any export. Nothing was deployed or started.');
      status('A connected flow was imported. Check its source and destination compatibility before deployment.');
    } else { $('platform-result').textContent = JSON.stringify(safeLiveSummary(result), null, 2); $('platform-result').hidden = false; platformStatus('Inspection complete. No platform workload was modified.'); }
  } catch (error) { platformStatus(error.message || 'The platform request could not be completed.', true); }
  finally { platformBusy = false; platformControls(); }
}
$('platform-form').addEventListener('submit', event => { event.preventDefault(); platformOperation('inspect'); });
$('platform-export').addEventListener('click', () => platformOperation('export'));
$('platform-url').addEventListener('input', () => { $('platform-bearer').value = ''; $('platform-result').hidden = true; });
$('platform-kind').addEventListener('change', () => {
  const kind = $('platform-kind').value;
  for (const [key, id] of [['nifi', 'platform-nifi-fields'], ['camel-k', 'platform-camel-fields'], ['seatunnel', 'platform-seatunnel-fields']]) $(id).hidden = kind !== key;
  $('platform-bearer').value = ''; $('platform-result').hidden = true;
  $('platform-url-hint').textContent = {nifi:'Use the NiFi REST API base URL, including /nifi-api. Certificate verification stays enabled.', 'camel-k':'Use the Kubernetes API server base URL. The platform token needs read access to the named Camel K integration.', seatunnel:'Use the SeaTunnel REST API base URL. This release can inspect job state; flow export is not supported.'}[kind];
  platformStatus(kind === 'seatunnel' ? 'SeaTunnel status inspection is available. Flow export is unsupported in this release.' : 'Enter the platform endpoint and resource to inspect or fetch.');
  platformControls();
});
window.addEventListener('pagehide', () => { $('platform-bearer').value = ''; });
platformControls();

function showGraph(data) {
  const analysis = data.assessment, graph = analysis.graph || {};
  const processors = graph.processors || [], groups = graph.groups || [], connections = graph.connections || [];
  $('graph-assessment').hidden = false;
  $('graph-summary').textContent = `${processors.length} processors · ${groups.length} groups · ${connections.length} connections. Source version: ${JSON.stringify(analysis.source_version)}.`;
  const body = $('graph-components'); body.replaceChildren();
  for (const processor of processors) {
    const row = document.createElement('tr');
    for (const value of [processor.name || processor.id || processor.identifier, processor.type, processor.group_id || processor.group || processor.groupIdentifier || '—']) { const cell = document.createElement('td'); cell.textContent = typeof value === 'object' ? JSON.stringify(value) : String(value || '—'); row.append(cell); }
    body.append(row);
  }
  $('graph-details').textContent = JSON.stringify({version:analysis.source_version,capabilities:analysis.capabilities,diagnostics:analysis.diagnostics,upgrade_plan:data.upgrade_plan}, null, 2);
}

$('http-example-button').addEventListener('click', async () => {
  if (busy) return;
  setBusy(true);
  try {
    const response = await fetch('/api/example/http');
    if (!response.ok) throw new Error('The HTTP example is unavailable.');
    $('document').value = JSON.stringify(await response.json(), null, 2);
    $('source').value = 'nifi'; $('nifi-version').value = '2'; $('target').value = 'airflow';
    $('target-description').textContent = descriptions.airflow; $('batch-contract-label').hidden = false; $('batch-contract').checked = false;
    $('file-detail').textContent = 'Synthetic four-processor HTTPS ingestion example'; invalidate();
    status('HTTP example loaded. Assess the full flow, then review and acknowledge batch scheduling before export.');
  } catch (error) { status(error.message, true); }
  finally { setBusy(false); }
});

$('media-example-button').addEventListener('click', async () => {
  if (busy) return;
  setBusy(true);
  try {
    const response = await fetch('/api/example/media');
    if (!response.ok) throw new Error('The media example is unavailable.');
    $('document').value = JSON.stringify(await response.json(),null,2);
    $('source').value='auto'; $('nifi-version').value='auto'; $('target').value='nifi';
    $('target-description').textContent='Generate the three-lane NiFi integration template. Review configuration and semantic gaps before attempting a native import.';
    $('batch-contract-label').hidden=true; $('partial-ack').checked=false;
    $('file-detail').textContent='Synthetic image, text and video ETL blueprint'; invalidate();
    $('media-demo').hidden=false;
    const proofResponse = await fetch('/api/demo/media/evidence');
    if (!proofResponse.ok) throw new Error('The recorded demo evidence is unavailable.');
    const evidence = await proofResponse.json();
    $('media-proof-summary').textContent = `Recorded ${evidence.recorded_at}: ${evidence.proof.complete} completed jobs, ${evidence.proof.dead_letter} dead-letter records, ${evidence.proof.kafka_events_read_back} Kafka events verified.`;
    for (const [id,values] of [['media-success',evidence.success_cases],['media-failures',evidence.failure_cases]]) { const list=$(id);list.replaceChildren();for (const value of values || []) {const item=document.createElement('li');item.textContent=value;list.append(item);} }
    const nativeResponse = await fetch('/api/demo/media/native-import');
    const native = nativeResponse.ok ? await nativeResponse.json() : null;
    if (native?.native_import_executed) $('media-proof-summary').textContent += ` Native NiFi ${native.nifi_version}: ${native.processors.length} processors and ${native.connections} connections imported; ETL was not started.`;
    const targetResponse = await fetch('/api/demo/media/target-imports');
    const targets = targetResponse.ok ? await targetResponse.json() : null;
    if (targets?.camel?.status === 'passed') $('media-proof-summary').textContent += ' Camel: three routes loaded without starting them. SeaTunnel: syntax checked only; runtime import remains unverified.';
    const offlineResponse = await fetch('/api/demo/airgap/evidence');
    const offline = offlineResponse.ok ? await offlineResponse.json() : null;
    if (offline?.result === 'passed') $('media-proof-summary').textContent += ' Separate offline test: two storage endpoints, four completed jobs, one retained failure, six Kafka events; public TCP probes blocked. Physical air-gap certification is outside this test.';
    $('media-evidence').textContent=JSON.stringify({reference_execution:evidence,native_import:native,target_imports:targets,offline_test:offline},null,2);
    status('Media blueprint loaded. Choose a target and review its mapping coverage. Partial targets require the incomplete-package acknowledgement.');
  } catch (error) { status(error.message,true); }
  finally {setBusy(false);}
});

$('continuous-example-button').addEventListener('click', async () => {
  if (busy) return;
  setBusy(true);
  try {
    const response = await fetch('/api/example/continuous');
    if (!response.ok) throw new Error('The tested native NiFi example is unavailable.');
    $('document').value = JSON.stringify(await response.json(), null, 2);
    $('source').value = 'nifi'; $('nifi-version').value = '2'; $('target').value = 'continuous-worker';
    $('target-description').textContent = descriptions['continuous-worker'];
    $('batch-contract-label').hidden = true; $('partial-ack').checked = false;
    $('file-detail').textContent = 'Sanitized native NiFi export from the continuous migration test'; invalidate();
    status('Native flow loaded. Assess its full inventory, then prepare the continuous-worker export and review the cutover instructions.');
  } catch (error) { status(error.message, true); }
  finally { setBusy(false); }
});

(async () => {
  try {
    const response = await fetch('/api/demo/continuous/evidence');
    if (!response.ok) throw new Error('Evidence unavailable');
    const evidence = await response.json();
    $('continuous-proof-data').textContent = JSON.stringify(evidence, null, 2);
    $('continuous-proof-summary').textContent = evidence.result === 'passed' ? `Recorded run: ${evidence.native_completed_at_drain} files processed by NiFi + ${evidence.target_processed} by the generated worker. ${evidence.total_destination_objects} destination files verified; ${evidence.missing_objects} missing. Arrivals continued during handover. This verifies the tested three-lane metadata-classification mapping, not arbitrary transformations or every target platform.` : 'This recorded run did not pass. Review the evidence before making a migration claim.';
  } catch {
    $('continuous-proof-summary').textContent = 'Recorded evidence is unavailable in this deployment. No completed migration is claimed here.';
  }
})();
