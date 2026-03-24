"""Cute NOAA JSON Data showcase API.

Separate from the existing API, this app focuses on neat visual browsing:
- Friendly dashboard at /
- Combined and per-file JSON endpoints
- Clean cards + table presentation
"""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

from noaa_json_loader import NoaaJsonLoader


app = FastAPI(title="NOAA JSON Showcase API", version="1.0.0")
loader = NoaaJsonLoader()


def _safe_reload():
    loaded = loader.load_all()
    if loaded.combined_dataframe.empty and not loaded.file_dataframes:
        raise HTTPException(status_code=404, detail="No NOAA JSON data found")
    return loaded


@app.get("/api/summary")
def summary():
    loaded = _safe_reload()
    return {
        "source_dir": str(loaded.source_dir),
        "file_count": len(loaded.file_dataframes),
        "combined_rows": int(len(loaded.combined_dataframe)),
        "columns": list(loaded.combined_dataframe.columns),
    }


@app.get("/api/files")
def list_files():
    loaded = _safe_reload()
    rows = []
    for name, frame in loaded.file_dataframes.items():
        rows.append({
            "file": name,
            "rows": int(len(frame)),
            "columns": list(frame.columns),
        })
    return {"files": rows}


@app.get("/api/files/{filename}")
def file_rows(filename: str, limit: int = 50):
    loaded = _safe_reload()
    frame = loaded.file_dataframes.get(filename)
    if frame is None:
        raise HTTPException(status_code=404, detail=f"File not loaded: {filename}")
    return {
        "file": filename,
        "rows": int(len(frame)),
        "preview": frame.head(limit).to_dict(orient="records"),
    }


@app.get("/api/combined")
def combined(limit: int = 100):
    loaded = _safe_reload()
    frame = loaded.combined_dataframe
    return {
        "rows": int(len(frame)),
        "preview": frame.head(limit).to_dict(orient="records"),
    }


@app.get("/", response_class=HTMLResponse)
def home() -> str:
    return """
<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\" />
  <title>NOAA JSON Data Showcase</title>
  <style>
    :root {
      --bg: #f6fbff;
      --card: #ffffff;
      --ink: #1c2a39;
      --muted: #5f7387;
      --accent: #2f80ed;
      --accent-soft: #d9e9ff;
      --border: #d8e3ef;
    }
    body { margin: 0; font-family: Inter, Arial, sans-serif; background: var(--bg); color: var(--ink); }
    .wrap { max-width: 1080px; margin: 24px auto; padding: 0 16px; }
    .hero { display: flex; justify-content: space-between; align-items: center; gap: 16px; margin-bottom: 16px; }
    .title { font-size: 28px; font-weight: 700; }
    .sub { color: var(--muted); font-size: 14px; }
    .cards { display: grid; grid-template-columns: repeat(auto-fit,minmax(180px,1fr)); gap: 12px; margin-bottom: 16px; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 12px; box-shadow: 0 2px 8px rgba(25,45,70,.05); }
    .label { color: var(--muted); font-size: 12px; }
    .value { font-size: 24px; font-weight: 700; margin-top: 4px; }
    .panel { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 12px; }
    .row { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; margin-bottom: 10px; }
    select, button { border: 1px solid var(--border); background: #fff; border-radius: 8px; padding: 8px 10px; }
    button { background: var(--accent); color: #fff; border-color: var(--accent); cursor: pointer; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { border-bottom: 1px solid var(--border); padding: 8px; text-align: left; vertical-align: top; }
    th { background: var(--accent-soft); }
    .muted { color: var(--muted); }
  </style>
</head>
<body>
  <div class=\"wrap\">
    <div class=\"hero\">
      <div>
        <div class=\"title\">NOAA JSON Data Showcase</div>
        <div class=\"sub\">Separate loader + neat API browser for your NOAA JSON files.</div>
      </div>
      <button onclick=\"refreshAll()\">Refresh</button>
    </div>

    <div class=\"cards\">
      <div class=\"card\"><div class=\"label\">Source Folder</div><div id=\"source\" class=\"value muted\" style=\"font-size:14px\">...</div></div>
      <div class=\"card\"><div class=\"label\">Files</div><div id=\"fileCount\" class=\"value\">0</div></div>
      <div class=\"card\"><div class=\"label\">Combined Rows</div><div id=\"rowCount\" class=\"value\">0</div></div>
      <div class=\"card\"><div class=\"label\">Columns</div><div id=\"colCount\" class=\"value\">0</div></div>
    </div>

    <div class=\"panel\">
      <div class=\"row\">
        <label for=\"filePicker\" class=\"muted\">View file:</label>
        <select id=\"filePicker\"></select>
        <button onclick=\"loadFilePreview()\">Load File Preview</button>
        <button onclick=\"loadCombinedPreview()\">Load Combined Preview</button>
        <button onclick="downloadCurrentCsv()">Download Current CSV</button>
      </div>
      <div id=\"status\" class=\"muted\" style=\"margin-bottom:8px\"></div>
      <table id=\"table\"></table>
    </div>
  </div>

  <script>
    const statusEl = document.getElementById('status');
    const filePicker = document.getElementById('filePicker');
    const table = document.getElementById('table');
    let currentRows = [];
    let currentViewName = 'combined_preview';

    function setStatus(msg) { statusEl.textContent = msg; }

    function escapeCsv(value) {
      const text = (value ?? '').toString();
      if (text.includes(',') || text.includes('"') || text.includes('\n')) {
        return '"' + text.replaceAll('"', '""') + '"';
      }
      return text;
    }

    function downloadCurrentCsv() {
      if (!currentRows.length) {
        setStatus('No rows loaded yet. Load a preview first.');
        return;
      }

      const cols = Object.keys(currentRows[0]);
      const header = cols.map(escapeCsv).join(',');
      const lines = currentRows.map(row => cols.map(col => escapeCsv(row[col])).join(','));
      const csvText = [header, ...lines].join('\n');

      const blob = new Blob([csvText], { type: 'text/csv;charset=utf-8;' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      const fileName = `${currentViewName}.csv`;
      link.href = url;
      link.download = fileName;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
      setStatus(`Downloaded ${fileName}`);
    }

    function renderTable(rows) {
      currentRows = rows || [];
      if (!rows || !rows.length) {
        table.innerHTML = '<tr><td>No rows to display.</td></tr>';
        return;
      }
      const cols = Object.keys(rows[0]);
      const head = '<thead><tr>' + cols.map(c => `<th>${c}</th>`).join('') + '</tr></thead>';
      const body = '<tbody>' + rows.map(r => '<tr>' + cols.map(c => `<td>${(r[c] ?? '').toString()}</td>`).join('') + '</tr>').join('') + '</tbody>';
      table.innerHTML = head + body;
    }

    async function refreshSummary() {
      const resp = await fetch('/api/summary');
      const data = await resp.json();
      document.getElementById('source').textContent = data.source_dir || 'unknown';
      document.getElementById('fileCount').textContent = data.file_count ?? 0;
      document.getElementById('rowCount').textContent = data.combined_rows ?? 0;
      document.getElementById('colCount').textContent = (data.columns || []).length;
    }

    async function refreshFiles() {
      const resp = await fetch('/api/files');
      const data = await resp.json();
      filePicker.innerHTML = '';
      for (const row of data.files || []) {
        const opt = document.createElement('option');
        opt.value = row.file;
        opt.textContent = `${row.file} (${row.rows} rows)`;
        filePicker.appendChild(opt);
      }
    }

    async function loadFilePreview() {
      const file = filePicker.value;
      if (!file) {
        setStatus('No file selected.');
        return;
      }
      const resp = await fetch(`/api/files/${encodeURIComponent(file)}?limit=50`);
      const data = await resp.json();
      if (!resp.ok) {
        setStatus(data.detail || 'Could not load file preview.');
        return;
      }
      currentViewName = data.file.replace(/\\.json$/i, '') + '_preview';
      setStatus(`Showing preview for ${data.file} (${data.rows} total rows)`);
      renderTable(data.preview || []);
    }

    async function loadCombinedPreview() {
      const resp = await fetch('/api/combined?limit=100');
      const data = await resp.json();
      if (!resp.ok) {
        setStatus(data.detail || 'Could not load combined preview.');
        return;
      }
      currentViewName = 'combined_preview';
      setStatus(`Showing combined preview (${data.rows} total rows)`);
      renderTable(data.preview || []);
    }

    async function refreshAll() {
      try {
        await refreshSummary();
        await refreshFiles();
        await loadCombinedPreview();
      } catch (err) {
        setStatus('Error loading dashboard.');
      }
    }

    refreshAll();
  </script>
</body>
</html>
    """
