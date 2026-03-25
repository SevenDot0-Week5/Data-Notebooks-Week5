# NOAA Daily Summaries - Implementation Guide

This guide explains both:

1. how this project works now, and
2. how to build a similar system from scratch.

---

## 1) System Overview

The system is intentionally split into clear layers:

- Fetch layer (`tokengrabber.py`)
  - Pulls paginated NOAA data and saves JSON files.
  - Uses Python standard library HTTP (`urllib`) so no external HTTP dependency is required.

- Load/transform layer (`json_helper.py`)
  - Reads all JSON pages and produces one pandas DataFrame.
  - Can optionally fetch latest NOAA data before loading.

- Runtime script (`build_daily_summaries_df.py`)
  - Creates a DataFrame in one command for quick validation.

- Notebook layer (`loading_and_graphing_daily_summaries.ipynb`)
  - Uses the same helper functions for analysis and graphs.
  - Prints whether it loaded from live API or local cache.

- API layer (`dataframe_api.py`)
  - Provides row-level view/edit/delete endpoints.
  - Persists working changes to CSV so edits survive restarts.
  - Includes a simple browser UI at `/ui`.

---

## 2) File-by-File Purpose

### `tokengrabber.py`
- Responsibility: NOAA download + local JSON persistence.
- Key behavior:
  - Reads token from argument/env (`NOAA_TOKEN`).
  - Handles pagination (`offset`, `limit`).
  - Saves each response page as a JSON file.

### `json_helper.py`
- Responsibility: DataFrame assembly and shared data flow.
- Key functions:
  - `load_json_files_to_dataframe(...)`
  - `fetch_and_load_daily_summaries_dataframe(...)`

### `build_daily_summaries_df.py`
- Responsibility: quick command-line smoke check for DataFrame creation.

### `dataframe_api.py`
- Responsibility: serve DataFrame rows over HTTP and allow mutation.
- Also serves browser UI (`GET /ui`) for no-code interaction.

### `IMPLEMENTATION_GUIDE.md`
- Responsibility: architectural and operational reference.

---

## 3) API Contract (Current)

Base URL: `http://127.0.0.1:8010`

- `GET /`
  - Health/status, row count, columns, csv path.

- `GET /ui`
  - Simple browser interface for view/edit/delete/reload/refresh.

- `GET /rows?offset=0&limit=50`
  - Paged row list.

- `GET /rows/{row_id}`
  - Single row by positional id.

- `PATCH /rows/{row_id}`
  - Body: `{"updates": {"column": "value"}}`
  - Updates specific columns and persists.

- `DELETE /rows/{row_id}`
  - Deletes row and reindexes DataFrame.

- `POST /reload-from-json`
  - Rebuilds DataFrame from local JSON pages.

- `POST /refresh-from-api`
  - Body: `{"token": "..."}` (optional if env has `NOAA_TOKEN`)
  - Fetches latest NOAA pages, reloads DataFrame, persists CSV.

---

## 4) How to Run Everything

### A) Fetch raw NOAA JSON pages
```bash
cd /Users/ethan/Projects/Data-Notebooks-Week5/NOAADailySummaries
export NOAA_TOKEN="your_token_here"
python tokengrabber.py
```

### B) Build DataFrame in terminal
```bash
python build_daily_summaries_df.py
```

### C) Use notebook analysis
Open `loading_and_graphing_daily_summaries.ipynb`, run top-to-bottom.

### D) Start API + UI
```bash
uvicorn dataframe_api:app --reload --port 8010
```

Open:

- `http://127.0.0.1:8010/docs`
- `http://127.0.0.1:8010/ui`

---

## 5) How to Build a Similar System Yourself

Use this blueprint in any domain (finance logs, IoT data, CRM records, etc.).

### Step 1: Define a raw data source
- API endpoint(s)
- auth method (token/key/oauth)
- pagination strategy
- expected record schema

### Step 2: Build a fetch module
- Keep fetch logic isolated (`fetch_module.py`).
- Save raw payloads to disk for reproducibility.
- Include retry/error handling and timeout.

### Step 3: Build a normalizer/helper module
- Convert raw payload pages into a unified table/DataFrame.
- Centralize parsing in one place so notebook/API share logic.

### Step 4: Add a script runner
- Build one command that fetches + loads + prints sanity output.
- This catches issues before UI/API complexity.

### Step 5: Add CRUD API over working dataset
- Use a store class with methods: list/get/update/delete.
- Validate input columns and row ids.
- Persist changes to an editable storage format (CSV/SQLite).

### Step 6: Add a minimal UI
- Start with server-rendered or embedded HTML + fetch calls.
- Reuse API endpoints (no duplicate business logic in UI).

### Step 7: Add docs and examples
- Document architecture and endpoint contracts.
- Include common commands and troubleshooting notes.

---

## 6) Why This Architecture Works

1. Separation of concerns
- Fetch, parse, analysis, and CRUD are decoupled.

2. Reuse and consistency
- Notebook, scripts, and API all use shared helper functions.

3. Easy debugging
- Raw JSON is stored locally, so you can inspect source responses.

4. Safe iteration
- Mutable working CSV keeps edits separate from original JSON snapshots.

5. Low setup burden
- Browser UI enables basic operations without extra frontend tooling.

---

## 7) Data/Storage Strategy

- Immutable-ish source snapshots: `data/daily_summaries/*.json`
- Mutable working state: `data/daily_summaries/daily_summaries_working.csv`

Recommended practice:

- Treat JSON as source-of-truth snapshots.
- Treat CSV as an editable view/cache for API operations.

---

## 8) Testing Checklist for Similar Projects

- Fetch layer
  - Invalid token handling
  - Timeout and HTTP error handling
  - Pagination correctness (no missing/duplicate pages)

- Helper layer
  - Empty directory handling
  - Bad JSON handling
  - Expected columns present

- API layer
  - 200 for happy paths
  - 400 for bad payloads/unknown columns
  - 404 for out-of-range row ids
  - persistence after restart

- UI layer
  - pagination buttons
  - edit/delete success and error paths

---

## 9) Common Troubleshooting

- "Missing NOAA token"
  - Set `NOAA_TOKEN` before fetch/refresh.

- API shows stale data
  - Use `POST /refresh-from-api` or `/reload-from-json`.

- Row ids changed after delete
  - Expected behavior: DataFrame is reindexed after row deletion.

- Notebook vs API values differ
  - Notebook may be reading JSON while API is reading working CSV.
  - Decide whether to reload from JSON or work from CSV state.

---

## 10) Optional Next Improvements

- Add `POST /rows` for creating new rows.
- Add column type validation/coercion.
- Add audit log for edits/deletes.
- Swap CSV persistence for SQLite for stronger multi-user safety.
- Add authentication if exposed beyond local machine.

---

## 11) Code Reading Map (Follow This Order)

All core scripts are now commented so a new developer can follow intent quickly.

Suggested reading order:

1. `tokengrabber.py`

- Start here to understand API fetch, pagination, and raw JSON storage.

2. `json_helper.py`

- Next, see how raw JSON pages are normalized into one DataFrame.

3. `build_daily_summaries_df.py`

- Then review the one-command script that runs the shared helper flow.

4. `dataframe_api.py`

- Finally, inspect store + endpoints + embedded UI behavior.

If you follow this order, the architecture mirrors how data moves at runtime:

fetch -> save JSON -> load DataFrame -> expose API/UI.

---

## 12) Reusable Template Skeleton (For New Projects)

Use this directory layout for a similar system:

```text
your-project/
  data/
    raw/
    working/
  fetcher.py
  data_helper.py
  build_dataframe.py
  dataframe_api.py
  notebooks/
    analysis.ipynb
  IMPLEMENTATION_GUIDE.md
```

Starter responsibilities:

- `fetcher.py`
  - Fetch external API pages
  - Save raw payloads to `data/raw`

- `data_helper.py`
  - Load raw files and normalize to DataFrame
  - Provide one-call `fetch_and_load(...)`

- `build_dataframe.py`
  - Command-line smoke test and schema preview

- `dataframe_api.py`
  - `GET /rows`, `PATCH /rows/{id}`, `DELETE /rows/{id}`
  - Optional `GET /ui` for no-code interaction

- `notebooks/analysis.ipynb`
  - Keep analysis separate from API concerns

Why this skeleton scales:

- Keeps ingestion, transformation, and serving independent.
- Allows you to swap storage (CSV -> SQLite) with minimal code churn.
- Makes onboarding easier because each file has one primary responsibility.

---

## 13) README-Compatible Basic Files (Assignment-Safe)

If you want the most direct lab-style path, use these simplified files:

- `tokengrabber_basic.py`
  - Executes exactly the two README calls (offset 1 and 1001).
  - Saves exactly these files:
    - `daily_summaries_FIPS10003_jan_2018_0.json`
    - `daily_summaries_FIPS10003_jan_2018_1.json`

- `json_helper_basic.py`
  - Minimal helper that reads `results` from JSON pages and returns one DataFrame.

- `readme_requirements_check.py`
  - Verifies required JSON files exist.
  - Verifies DataFrame can be built.
  - Verifies notebook-required columns (`date`, `datatype`, `value`).

These files are kept separate so advanced API/UI work does not interfere with the original lab flow.

---

## 14) Exercise Checkpoints (Pass/Fail)

Use these checkpoints as a submission confidence checklist.

### Exercise 2 checkpoint
Run:

```bash
export NOAA_TOKEN="your_token_here"
python tokengrabber_basic.py
```
Pass when both files exist:

- `data/daily_summaries/daily_summaries_FIPS10003_jan_2018_0.json`
- `data/daily_summaries/daily_summaries_FIPS10003_jan_2018_1.json`

### Exercise 3 checkpoint
Run:

```bash
python readme_requirements_check.py
```
Pass when output contains:

- `README_REQUIREMENTS_CHECK_PASS`

### Exercise 4 checkpoint
In notebook:

- import helper module
- create `df_daily_summaries`
- create `temps_max` and `temps_min`
- produce min/max plots

Pass when:

- DataFrame loads without errors
- both `temps_max` and `temps_min` exist and have rows
- both graphs render

---

## 15) Basic to Advanced: What Changes and Why

This section explains how the assignment-safe path grows into the production-style path.

### A) Side-by-side mental model

- Basic path (good for class requirements)
  - `tokengrabber_basic.py`
  - `json_helper_basic.py`
  - Notebook analysis

- Advanced path (good for reusable tooling)
  - `tokengrabber.py`
  - `json_helper.py`
  - `dataframe_api.py` + `/ui`
  - Optional refresh/reload flows

Think of it as:

- Basic = complete the exercises clearly and directly
- Advanced = keep the same core logic, but package it for repeated use and editing

### B) File mapping (basic -> advanced)

- Fetch script
  - Basic: `tokengrabber_basic.py` (exactly 2 fixed calls)
  - Advanced: `tokengrabber.py` (parameterized + paginated until done)
  - Why upgrade:
    - supports other dates/locations without rewriting code
    - less manual maintenance

- JSON-to-DataFrame helper
  - Basic: `json_helper_basic.py` (minimal results loader)
  - Advanced: `json_helper.py` (shared helper used by script/notebook/API)
  - Why upgrade:
    - one source of truth for parsing
    - easier debugging and reuse

- Data interaction
  - Basic: notebook cells only
  - Advanced: API endpoints + browser UI (`/ui`)
  - Why upgrade:
    - view/edit/delete data without touching notebook code
    - easier demos and team usage

### C) Practical upgrade path (small steps)

1. Start with basic files

- Run `tokengrabber_basic.py`
- Run notebook with `json_helper_basic.py`

2. Swap helper import in notebook

- Move from `json_helper_basic` to `json_helper`
- Confirm same DataFrame shape and columns

3. Use advanced fetcher when needed

- Replace fixed calls with parameterized `tokengrabber.py`
- Keep outputs in same data folder

4. Add API only when you need mutable workflows

- Start `dataframe_api.py`
- Use `/rows` and `/ui` for row operations

### D) How to choose quickly

Use basic when:

- You need to satisfy README exercises exactly
- You want the least abstraction while learning

Use advanced when:

- You will rerun this often with different inputs
- You want an API/UI to inspect or edit rows
- You want clearer long-term structure for future projects

### E) Why this progression is a good engineering habit

- You reduce risk early
  - basic scripts prove requirements first

- You avoid overengineering too soon
  - no API/UI complexity until needed

- You still end with reusable architecture
  - shared helper + API/UI for growth

In short: basic gives confidence, advanced gives leverage.

---

## 16) End-to-End Runtime Walkthrough (What Happens Internally)

This section traces the exact data path through the system.

### Flow A: Basic assignment path

1. You run:

```bash
python tokengrabber_basic.py
```
2. Script sends two NOAA requests (`offset=1`, `offset=1001`).
3. Responses are saved to:

  - `data/daily_summaries/daily_summaries_FIPS10003_jan_2018_0.json`
  - `data/daily_summaries/daily_summaries_FIPS10003_jan_2018_1.json`
4. Notebook imports `json_helper_basic.py`.
5. `json_helper_basic.load_json_files_to_dataframe()` reads both JSON files.
6. It extracts `payload["results"]` rows and concatenates to one DataFrame.
7. Notebook analysis cells filter `datatype` into `TMAX` and `TMIN` and plot.

### Flow B: Advanced reusable path

1. You run:

```bash
uvicorn dataframe_api:app --reload --port 8010
```
2. `DataFrameStore` initializes in `dataframe_api.py`:

  - If `daily_summaries_working.csv` exists -> loads CSV.
  - Else -> loads JSON via `json_helper.load_json_files_to_dataframe()` and writes CSV.
3. API serves:

  - `/rows` for data listing
  - `/rows/{row_id}` for row-level operations
  - `/ui` as browser client that calls those endpoints
4. If you click “Refresh From API” in `/ui`:

  - API calls `json_helper.fetch_and_load_daily_summaries_dataframe()`
  - which calls `tokengrabber.fetch_daily_summaries()`
  - which fetches and stores fresh JSON pages
  - then helper reloads DataFrame from JSON
  - store writes updated CSV

### Why this matters

- You always know which file is source snapshot (JSON) vs editable state (CSV).
- You can reset API state from JSON if edits went wrong.

---

## 17) Notebook Cell Map (Exercise 4, line of thought)

Use this to understand intent behind each notebook block.

1. Import helper
- Goal: give notebook access to shared data loader.

2. Build `df_daily_summaries`
- Goal: one canonical DataFrame for all analysis.

3. Quick DataFrame summary
- Goal: verify shape, columns, and sanity before filtering.

4. Count stations for FIPS10003
- Goal: answer README prompt using DataFrame filtering.

5. Build `temps_max`
- Goal: isolate max-temperature rows (`datatype == "TMAX"`).

6. Stats on `temps_max`
- Goal: count, mean, min, max.

7. Plot `temps_max`
- Goal: visualize daily max pattern for month.

8. Build `temps_min`
- Goal: isolate min-temperature rows (`datatype == "TMIN"`).

9. Stats on `temps_min`
- Goal: same summary metrics for min temps.

10. Plot `temps_min` and combined plot
- Goal: compare min and max lines together.

---

## 18) API Endpoint Examples (Concrete Requests + Meaning)

### List rows
```bash
curl "http://127.0.0.1:8010/rows?offset=0&limit=2"
```
What this does:

- Reads two rows from current working DataFrame state.

### Edit one value
```bash
curl -X PATCH "http://127.0.0.1:8010/rows/0" \
  -H "Content-Type: application/json" \
  -d '{"updates":{"value":250}}'
```
What this does:

- Modifies row `0`, column `value`, then persists CSV.

### Delete one row
```bash
curl -X DELETE "http://127.0.0.1:8010/rows/1"
```
What this does:

- Removes row `1`, reindexes row ids, persists CSV.

### Reset edits from JSON snapshots
```bash
curl -X POST "http://127.0.0.1:8010/reload-from-json"
```
What this does:

- Replaces current working CSV state with JSON-based state.

### Pull fresh NOAA data + rebuild state
```bash
curl -X POST "http://127.0.0.1:8010/refresh-from-api" \
  -H "Content-Type: application/json" \
  -d '{"token":"your_token_here"}'
```
What this does:

- Downloads latest pages, rebuilds DataFrame, overwrites working CSV.

---

## 19) Basic vs Advanced: Decision Matrix

Choose based on your immediate goal:

- I need to submit class lab quickly and clearly -> Basic
- I want low moving parts while learning -> Basic
- I need reusable tools for repeated runs -> Advanced
- I want browser-based row editing -> Advanced
- I need to support changing parameters (dates/location) often -> Advanced

Simple rule:

- Start Basic.
- Move to Advanced when repetition or data operations become painful.

---

## 20) Troubleshooting Playbook (Step-by-Step)

### Problem: DataFrame won’t load in notebook
1. Run:

```bash
python readme_requirements_check.py
```
2. If files missing, run:

```bash
export NOAA_TOKEN="your_token_here"
python tokengrabber_basic.py
```
3. Re-run notebook from top.

### Problem: API starts but rows are empty
1. Check JSON files exist in `data/daily_summaries`.
2. Call:
```bash
curl -X POST "http://127.0.0.1:8010/reload-from-json"
```
3. Retry `/rows`.

### Problem: API edits look wrong

1. Reset from JSON snapshots:

```bash
curl -X POST "http://127.0.0.1:8010/reload-from-json"
```

2. Re-apply edits carefully.

### Problem: Token errors on refresh

1. Ensure token is valid.
2. Pass token in request body, or export `NOAA_TOKEN`.
3. Retry `/refresh-from-api`.

---

## 21) Recommended Learning Path (Fastest Understanding)

If your goal is understanding, run these in order:

1. `python tokengrabber_basic.py`
2. `python readme_requirements_check.py`
3. Run notebook top-to-bottom (basic helper)
4. Start API and open `/ui`
5. Edit one row, then reload from JSON
6. Refresh from API once

After this sequence, you will have seen every major data transition:

- API response -> JSON files -> DataFrame -> plots -> API edits -> reset -> refresh.

---

## 22) Explanation Coverage Checklist (What You Should Be Able to Explain)

Use this checklist before submission or presentation.

### Requirements mapping

- You can explain how each README exercise is satisfied (Exercise 1-4).
- You can point to the exact files used for the basic submission path.

### Data flow

- You can explain where raw data lives (`data/daily_summaries/*.json`).
- You can explain where editable working state lives (`daily_summaries_working.csv`).
- You can explain the sequence: fetch -> save JSON -> load DataFrame -> analyze/serve.

### Design choices

- You can explain why we kept basic and advanced files separate.
- You can explain why helper logic is centralized in `json_helper.py`.
- You can explain why API edits persist to CSV.

### Operations

- You can run and explain `tokengrabber_basic.py` and `readme_requirements_check.py`.
- You can run notebook cells and interpret TMAX/TMIN plots.
- You can run API + UI and describe view/edit/delete behavior.

### Validation evidence

- You can show `README_REQUIREMENTS_CHECK_PASS`.
- You can show API health (`GET /`) and a sample `/rows` response.

If every line above is true, your explanation is complete and defensible.

---

## 23) Debugging Help (Deep-Dive)

This section is a practical debug manual when something does not run correctly.

### Quick triage order

1. Validate files and schema:

```bash
python readme_requirements_check.py
```

2. Validate DataFrame build:

```bash
python build_daily_summaries_df.py
```

3. Validate API boot:

```bash
uvicorn dataframe_api:app --reload --port 8010
```

4. Validate API routes:

```bash
curl "http://127.0.0.1:8010/"
curl "http://127.0.0.1:8010/rows?offset=0&limit=1"
```

### Symptom -> likely cause -> fix

#### A) `Missing NOAA token`

- Likely cause:
  - `NOAA_TOKEN` not set in current shell
- Fix:

```bash
export NOAA_TOKEN="your_token_here"
```

#### B) Notebook loads but has wrong row count or NaN first row

- Likely cause:
  - malformed/non-record JSON data included in DataFrame
- Fix:
  - rerun from top after helper updates
  - regenerate JSON files using basic fetcher
  - run checker again

#### C) `ModuleNotFoundError` in notebook kernel

- Likely cause:
  - package installed in one interpreter but notebook uses another
- Fix:
  - select correct kernel/interpreter in VS Code
  - install missing package in notebook environment

#### D) API `/rows` empty but JSON files exist

- Likely cause:
  - stale working CSV state
- Fix:

```bash
curl -X POST "http://127.0.0.1:8010/reload-from-json"
```

#### E) Edited values disappear after restart

- Likely cause:
  - edits were reset from JSON (`/reload-from-json`) after editing
- Fix:
  - avoid reload unless intentionally resetting
  - verify `daily_summaries_working.csv` exists and updates

#### F) API refresh fails (`/refresh-from-api`)

- Likely cause:
  - invalid token or network/API issue
- Fix:
  - test token with basic fetch script
  - retry with explicit token body in refresh request

### Notebook-specific reset procedure

When notebook state gets inconsistent:

1. Restart kernel
2. Run cells from top in order
3. Confirm `df_daily_summaries` shape before running analysis cells

### API-specific reset procedure

When API behavior looks inconsistent:

1. Stop server
2. Start server again
3. Call `/reload-from-json`
4. Re-test `/rows` and `/ui`

### What to capture if asking for help

Collect these four items:

1. Exact command you ran
2. Full error text/traceback
3. Output of `python readme_requirements_check.py`
4. Output of `GET /` from API

These four usually identify the root cause quickly.
