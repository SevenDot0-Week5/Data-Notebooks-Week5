# NOAA Daily Summaries Quickstart Cheat Sheet

## 1) Go to project folder

Command:

```bash
cd /Users/ethan/Projects/Data-Notebooks-Week5/NOAADailySummaries
```

Expected:

- Terminal prompt path ends with `.../NOAADailySummaries`

---

## 2) Fetch required README JSON files (basic path)

Command:

```bash
export NOAA_TOKEN="your_token_here"
python tokengrabber_basic.py
```

Expected:

- Output shows two saved files:
  - `daily_summaries_FIPS10003_jan_2018_0.json`
  - `daily_summaries_FIPS10003_jan_2018_1.json`

---

## 3) Verify README requirements are satisfied

Command:

```bash
python readme_requirements_check.py
```

Expected:

- `PASS: required JSON files exist`
- `PASS: DataFrame built with ... rows`
- `PASS: Required columns present: ['datatype', 'date', 'value']`
- `README_REQUIREMENTS_CHECK_PASS`

---

## 4) Build DataFrame from advanced helper flow

Command:

```bash
python build_daily_summaries_df.py
```

Expected:

- `DataFrame created successfully`
- `Rows: ...`
- `Columns: ...`
- First few rows printed

---

## 5) Run notebook (exercise analysis)

Command:

- Open `loading_and_graphing_daily_summaries.ipynb`
- Run cells top-to-bottom

Expected:

- `df_daily_summaries` created
- `temps_max` and `temps_min` created
- max/min plots render

---

## 6) Start DataFrame API

Command:

```bash
uvicorn dataframe_api:app --reload --port 8010
```

Expected:

- Uvicorn startup log
- API available on `http://127.0.0.1:8010`

---

## 7) Open API docs and browser UI

URLs:

- `http://127.0.0.1:8010/docs`
- `http://127.0.0.1:8010/ui`

Expected:

- `/docs`: interactive endpoint list
- `/ui`: table view with edit/delete controls

---

## 8) API smoke checks

Command:

```bash
curl "http://127.0.0.1:8010/"
```

Expected:

- JSON with `message`, `rows`, `columns`, `csv_path`

Command:

```bash
curl "http://127.0.0.1:8010/rows?offset=0&limit=2"
```

Expected:

- JSON with `total_rows`, `offset`, `limit`, and `rows` array

---

## 9) Edit one row (API)

Command:

```bash
curl -X PATCH "http://127.0.0.1:8010/rows/0" \
  -H "Content-Type: application/json" \
  -d '{"updates":{"value":250}}'
```

Expected:

- JSON with `message: row updated`
- Updated row payload

---

## 10) Delete one row (API)

Command:

```bash
curl -X DELETE "http://127.0.0.1:8010/rows/1"
```

Expected:

- JSON with `message: row deleted`
- `total_rows` decremented

---

## 11) Reset working state from JSON snapshots

Command:

```bash
curl -X POST "http://127.0.0.1:8010/reload-from-json"
```

Expected:

- JSON with `message: reloaded from local JSON`

---

## 12) Refresh data from NOAA API via API endpoint

Command:

```bash
curl -X POST "http://127.0.0.1:8010/refresh-from-api" \
  -H "Content-Type: application/json" \
  -d '{"token":"your_token_here"}'
```

Expected:

- JSON with `message: fetched from NOAA API and refreshed`
- `total_rows` returned

---

## Common quick fixes

If token missing:

```bash
export NOAA_TOKEN="your_token_here"
```

If README check fails:

```bash
python tokengrabber_basic.py
python readme_requirements_check.py
```

If API data looks wrong after edits:

```bash
curl -X POST "http://127.0.0.1:8010/reload-from-json"
```
