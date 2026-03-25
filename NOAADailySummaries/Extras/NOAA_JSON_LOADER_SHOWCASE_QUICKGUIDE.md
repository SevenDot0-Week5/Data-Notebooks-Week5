# NOAA JSON Loader + Showcase API Quick Guide

## What was added

- `noaa_json_loader.py`
  - Separate JSON loader module
  - Loads one DataFrame per file + one combined DataFrame
  - Supports both:
    - `NOAA Json Data/`
    - `data/daily_summaries/` (fallback)

- `noaa_json_loader_lab.ipynb`
  - Jupyter Lab notebook that imports and uses the separate loader

- `noaa_json_showcase_api.py`
  - Separate FastAPI app with:
    - neat dashboard (`/`)
    - JSON endpoints (`/api/summary`, `/api/files`, `/api/files/{filename}`, `/api/combined`)

---

## Use in Jupyter Lab

Open:

- `noaa_json_loader_lab.ipynb`

Run cells in order:

1. Load data with `load_noaa_json_dataframes()`
2. View combined DataFrame head
3. Inspect row/column counts per source file

---

## Run the cute web API

From `NOAADailySummaries/`:

```bash
uvicorn noaa_json_showcase_api:app --reload --port 8020
```

Open:

- `http://127.0.0.1:8020/` (dashboard)
- `http://127.0.0.1:8020/api/summary`
- `http://127.0.0.1:8020/api/files`
- `http://127.0.0.1:8020/api/combined?limit=20`

---

## Why this structure is useful

- Keeps loading logic reusable and independent from notebook/API layers.
- Lets notebooks and APIs share the same source parsing behavior.
- Gives a visual browser for data without changing the original assignment notebook.
