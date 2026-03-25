"""Minimal DataFrame CRUD API + browser UI.

This service loads NOAA data into a pandas DataFrame and exposes endpoints to
view, edit, and delete rows. It also includes a tiny embedded web UI at /ui.

Persistence strategy:
- Source snapshots stay in JSON files.
- Mutable working state is persisted to CSV.
"""

import os
from pathlib import Path
from threading import Lock
from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

import json_helper


BASE_DIR = Path(__file__).parent
DEFAULT_JSON_DIR = BASE_DIR / "data" / "daily_summaries"
DEFAULT_CSV_PATH = DEFAULT_JSON_DIR / "daily_summaries_working.csv"


class RowUpdateRequest(BaseModel):
    # PATCH payload shape: {"updates": {"column_name": value}}
    updates: dict[str, Any] = Field(default_factory=dict)


class DataFrameStore:
    """Thread-safe in-memory DataFrame wrapper with CSV persistence."""

    def __init__(self, csv_path: Path, json_dir: Path):
        self.csv_path = csv_path
        self.json_dir = json_dir
        self._lock = Lock()
        self._df = pd.DataFrame()
        self.load_initial()

    def load_initial(self) -> None:
        # On startup prefer editable CSV, otherwise bootstrap from JSON pages.
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        if self.csv_path.exists():
            self._df = pd.read_csv(self.csv_path)
        else:
            self._df = json_helper.load_json_files_to_dataframe(self.json_dir)
            self._save_locked()
        self._df = self._df.reset_index(drop=True)

    def _save_locked(self) -> None:
        # Persist current working DataFrame so edits/deletes survive restarts.
        self._df.to_csv(self.csv_path, index=False)

    def count(self) -> int:
        return len(self._df)

    def columns(self) -> list[str]:
        return list(self._df.columns)

    def list_rows(self, offset: int = 0, limit: int = 50) -> list[dict[str, Any]]:
        # Offset/limit paging keeps payload size manageable.
        end = offset + limit
        if offset < 0 or limit < 1:
            raise ValueError("offset must be >= 0 and limit must be >= 1")
        records = self._df.iloc[offset:end].to_dict(orient="records")
        return [self._with_row_id(offset + idx, row) for idx, row in enumerate(records)]

    def get_row(self, row_id: int) -> dict[str, Any]:
        self._assert_row_id(row_id)
        row = self._df.iloc[row_id].to_dict()
        return self._with_row_id(row_id, row)

    def update_row(self, row_id: int, updates: dict[str, Any]) -> dict[str, Any]:
        # Validate updates early to return useful API errors.
        if not updates:
            raise ValueError("updates cannot be empty")

        with self._lock:
            self._assert_row_id(row_id)
            unknown_columns = [column for column in updates if column not in self._df.columns]
            if unknown_columns:
                raise KeyError(f"Unknown columns: {', '.join(unknown_columns)}")

            for column, value in updates.items():
                self._df.at[row_id, column] = value

            self._save_locked()
            return self.get_row(row_id)

    def delete_row(self, row_id: int) -> dict[str, Any]:
        with self._lock:
            self._assert_row_id(row_id)
            deleted = self.get_row(row_id)
            # Reindex so row_id remains a contiguous positional index.
            self._df = self._df.drop(index=row_id).reset_index(drop=True)
            self._save_locked()
            return deleted

    def reload_from_json(self) -> int:
        # Replace working CSV state with source JSON snapshot state.
        with self._lock:
            self._df = json_helper.load_json_files_to_dataframe(self.json_dir).reset_index(drop=True)
            self._save_locked()
            return len(self._df)

    def fetch_reload_from_api(self, token: str | None) -> int:
        # Pull fresh NOAA data, then rebuild DataFrame from disk for consistency.
        if not token:
            raise ValueError("Missing NOAA token. Set NOAA_TOKEN or pass token in request body.")

        with self._lock:
            json_helper.fetch_and_load_daily_summaries_dataframe(token=token, directory_path=self.json_dir)
            self._df = json_helper.load_json_files_to_dataframe(self.json_dir).reset_index(drop=True)
            self._save_locked()
            return len(self._df)

    def _assert_row_id(self, row_id: int) -> None:
        if row_id < 0 or row_id >= len(self._df):
            raise IndexError(f"row_id out of range: {row_id}")

    @staticmethod
    def _with_row_id(row_id: int, row: dict[str, Any]) -> dict[str, Any]:
        result = dict(row)
        result["row_id"] = row_id
        return result


app = FastAPI(title="NOAA Daily Summaries DataFrame API", version="1.0.0")
store = DataFrameStore(DEFAULT_CSV_PATH, DEFAULT_JSON_DIR)


@app.get("/ui", response_class=HTMLResponse)
def ui() -> str:
    # Embedded UI intentionally stays simple (no build tooling required).
    return """
<!doctype html>
<html>
<head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\" />
    <title>NOAA DataFrame UI</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        h1 { margin-bottom: 8px; }
        .toolbar { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 12px; }
        button { padding: 6px 10px; cursor: pointer; }
        input { padding: 6px; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 6px; text-align: left; font-size: 13px; }
        th { background: #f4f4f4; }
        .muted { color: #666; font-size: 12px; }
        .actions button { margin-right: 4px; }
    </style>
</head>
<body>
    <h1>NOAA DataFrame Viewer</h1>
    <div class=\"muted\">Simple table UI for view, edit, and delete operations.</div>

    <div class=\"toolbar\">
        <button onclick=\"prevPage()\">Prev</button>
        <button onclick=\"nextPage()\">Next</button>
        <button onclick=\"loadRows()\">Reload View</button>
        <button onclick=\"reloadFromJson()\">Reload From JSON</button>
        <input id=\"token\" placeholder=\"NOAA token (optional)\" size=\"32\" />
        <button onclick=\"refreshFromApi()\">Refresh From API</button>
    </div>

    <div id=\"status\" class=\"muted\"></div>
    <table id=\"grid\"></table>

    <script>
        let offset = 0;
        const limit = 20;
        let columns = [];

        function setStatus(msg) {
            document.getElementById('status').textContent = msg;
        }

        async function loadRows() {
            const resp = await fetch(`/rows?offset=${offset}&limit=${limit}`);
            const data = await resp.json();
            if (!resp.ok) {
                setStatus(`Error: ${data.detail || 'failed to load rows'}`);
                return;
            }

            const rows = data.rows || [];
            renderTable(rows);
            const start = rows.length ? offset : 0;
            const end = rows.length ? offset + rows.length - 1 : 0;
            setStatus(`Showing ${start}-${end} of ${data.total_rows} total rows`);
        }

        function renderTable(rows) {
            const table = document.getElementById('grid');
            if (!rows.length) {
                table.innerHTML = '<tr><td>No rows</td></tr>';
                return;
            }

            columns = Object.keys(rows[0]).filter(k => k !== 'row_id');
            const head = `
                <thead>
                    <tr>
                        <th>row_id</th>
                        ${columns.map(c => `<th>${c}</th>`).join('')}
                        <th>actions</th>
                    </tr>
                </thead>`;

            const body = `
                <tbody>
                    ${rows.map(row => `
                        <tr>
                            <td>${row.row_id}</td>
                            ${columns.map(c => `<td>${row[c] ?? ''}</td>`).join('')}
                            <td class=\"actions\">
                                <button onclick=\"editRow(${row.row_id})\">Edit</button>
                                <button onclick=\"deleteRow(${row.row_id})\">Delete</button>
                            </td>
                        </tr>
                    `).join('')}
                </tbody>`;

            table.innerHTML = head + body;
        }

        async function editRow(rowId) {
            const col = prompt(`Column to edit:\n${columns.join(', ')}`);
            if (!col) return;
            const value = prompt(`New value for ${col}:`);
            if (value === null) return;

            const resp = await fetch(`/rows/${rowId}`, {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ updates: { [col]: value } })
            });
            const data = await resp.json();
            if (!resp.ok) {
                setStatus(`Edit failed: ${data.detail || 'error'}`);
                return;
            }
            setStatus(`Row ${rowId} updated`);
            await loadRows();
        }

        async function deleteRow(rowId) {
            const ok = confirm(`Delete row ${rowId}?`);
            if (!ok) return;

            const resp = await fetch(`/rows/${rowId}`, { method: 'DELETE' });
            const data = await resp.json();
            if (!resp.ok) {
                setStatus(`Delete failed: ${data.detail || 'error'}`);
                return;
            }
            setStatus(`Row ${rowId} deleted`);
            await loadRows();
        }

        async function reloadFromJson() {
            const resp = await fetch('/reload-from-json', { method: 'POST' });
            const data = await resp.json();
            if (!resp.ok) {
                setStatus(`Reload failed: ${data.detail || 'error'}`);
                return;
            }
            setStatus(data.message);
            offset = 0;
            await loadRows();
        }

        async function refreshFromApi() {
            const token = document.getElementById('token').value.trim();
            const body = token ? { token } : {};
            const resp = await fetch('/refresh-from-api', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body)
            });
            const data = await resp.json();
            if (!resp.ok) {
                setStatus(`Refresh failed: ${data.detail || 'error'}`);
                return;
            }
            setStatus(data.message);
            offset = 0;
            await loadRows();
        }

        function nextPage() {
            offset += limit;
            loadRows();
        }

        function prevPage() {
            offset = Math.max(0, offset - limit);
            loadRows();
        }

        loadRows();
    </script>
</body>
</html>
        """


@app.get("/")
def root() -> dict[str, Any]:
    # Quick diagnostics endpoint for health + current dataset shape.
    return {
        "message": "DataFrame API is running",
        "rows": store.count(),
        "columns": store.columns(),
        "csv_path": str(DEFAULT_CSV_PATH),
    }


@app.get("/rows")
def get_rows(offset: int = 0, limit: int = 50) -> dict[str, Any]:
    # Read-only row listing with pagination.
    try:
        rows = store.list_rows(offset=offset, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "total_rows": store.count(),
        "offset": offset,
        "limit": limit,
        "rows": rows,
    }


@app.get("/rows/{row_id}")
def get_row(row_id: int) -> dict[str, Any]:
    try:
        return store.get_row(row_id)
    except IndexError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.patch("/rows/{row_id}")
def patch_row(row_id: int, body: RowUpdateRequest) -> dict[str, Any]:
    # Partial update endpoint for one row.
    try:
        updated = store.update_row(row_id=row_id, updates=body.updates)
        return {"message": "row updated", "row": updated}
    except (ValueError, KeyError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except IndexError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.delete("/rows/{row_id}")
def delete_row(row_id: int) -> dict[str, Any]:
    # Delete row by positional id.
    try:
        deleted = store.delete_row(row_id=row_id)
        return {"message": "row deleted", "deleted": deleted, "total_rows": store.count()}
    except IndexError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/reload-from-json")
def reload_from_json() -> dict[str, Any]:
    count = store.reload_from_json()
    return {"message": "reloaded from local JSON", "total_rows": count}


class RefreshRequest(BaseModel):
    # Optional request token; falls back to NOAA_TOKEN env var.
    token: str | None = None


@app.post("/refresh-from-api")
def refresh_from_api(body: RefreshRequest) -> dict[str, Any]:
    token = body.token or os.getenv("NOAA_TOKEN")
    try:
        count = store.fetch_reload_from_api(token)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"message": "fetched from NOAA API and refreshed", "total_rows": count}
