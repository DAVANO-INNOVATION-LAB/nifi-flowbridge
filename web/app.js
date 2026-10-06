'use strict';
const $ = id => document.getElementById(id);
const MAX_BYTES = 2 * 1024 * 1024;
const descriptions = {
  seatunnel: 'Generate a SeaTunnel configuration for the supported flow, with deployment notes and a compatibility report.',
  'camel-k': 'Generate a Camel K integration for the supported flow. Review configuration before deploying to Kubernetes.',
  kafka: 'Generate a Kafka Streams project and an importable Flowbridge manifest. Kafka is the destination runtime, not a universal flow format.',
  nifi: 'Generate a NiFi flow definition for the supported subset. Review processors and connection settings before importing.',
  flowbridge: 'Export the normalized flow as an open, portable JSON document. Use it to inspect or re-import the supported flow.'
};
let preparedRequest = null;
let busy = false;
function status(message, error = false) { $('status').textContent = message; $('status').classList.toggle('error', error); }
function setBusy(value) { busy = value; for (const id of ['analyze-button', 'convert-button', 'example-button', 'clear-button']) $(id).disabled = value; $('download-button').disabled = value || !$('review-ack').checked || !preparedRequest; $('review-ack').disabled = value; $('source').disabled = value; $('target').disabled = value; $('document').readOnly = value; $('file-input').disabled = value; }
function invalidate() { preparedRequest = null; $('review-ack').checked = false; $('download-button').disabled = true; $('artifacts').hidden = true; $('report').hidden = true; status($('document').value.trim() ? 'Flow added. Check the source or prepare an export.' : 'Add a flow to begin.'); }
function requestBody() {
  const raw = $('document').value.trim();
  if (!raw) throw new Error('Add a JSON flow document first.');
  if (new TextEncoder().encode(raw).length > MAX_BYTES) throw new Error('This document exceeds the 2 MB limit.');
  let document; try { document = JSON.parse(raw); } catch { throw new Error('This is not valid JSON. Check the document and try again.'); }
  if (!document || typeof document !== 'object' || Array.isArray(document)) throw new Error('The flow document must be a JSON object.');
  return { source: $('source').value, target: $('target').value, document };
}
async function jsonRequest(path, body) {
  const response = await fetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  const data = await response.json().catch(() => null);
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
    const data = await jsonRequest(`/api/${mode}`, body);
    const errors = showReport(data.report || data, mode === 'analyze' ? 'source' : 'export');
    if (errors) { status('Resolve the compatibility blockers before exporting.', true); return; }
    if (mode === 'convert') { if (!showFiles(data.files)) throw new Error('No project files were generated. Review the report.'); preparedRequest = body; status('Export prepared. Review the findings and generated files below.'); }
    else status('Source checked. Prepare an export to check the selected destination.');
  } catch (error) { status(error.message || 'The request could not be completed.', true); }
  finally { setBusy(false); }
}
async function loadFile(file) {
  if (busy || !file) return;
  invalidate();
  if (file.size > MAX_BYTES) { status('Choose a JSON file smaller than 2 MB.', true); return; }
  try { $('document').value = await file.text(); $('file-detail').textContent = `${file.name} · ${(file.size / 1024).toFixed(1)} KB`; invalidate(); requestBody(); }
  catch (error) { status(error.message || 'The file could not be read.', true); }
}
$('file-input').addEventListener('change', event => loadFile(event.target.files[0]));
$('document').addEventListener('input', invalidate);
$('source').addEventListener('change', invalidate);
$('target').addEventListener('change', () => { $('target-description').textContent = descriptions[$('target').value]; invalidate(); });
$('clear-button').addEventListener('click', () => { $('document').value = ''; $('file-input').value = ''; $('file-detail').textContent = 'Up to 2 MB · processed on this local server'; invalidate(); $('document').focus(); });
$('analyze-button').addEventListener('click', () => run('analyze'));
$('convert-button').addEventListener('click', () => run('convert'));
$('review-ack').addEventListener('change', () => { $('download-button').disabled = busy || !$('review-ack').checked || !preparedRequest; });
$('example-button').addEventListener('click', async () => { if (busy) return; setBusy(true); try { const response = await fetch('/api/example'); if (!response.ok) throw new Error('The example could not be loaded.'); const data = await response.json(); $('document').value = JSON.stringify(data.document || data, null, 2); $('source').value = typeof data.source === 'string' ? data.source : 'auto'; $('file-detail').textContent = 'Example loaded · no credentials included'; invalidate(); } catch (error) { status(error.message, true); } finally { setBusy(false); } });
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
