"""Basic Exercise 3 module (README-aligned).

This intentionally simple helper focuses on the lab requirement:
- Read JSON files from data/daily_summaries
- Convert the NOAA `results` objects into one pandas DataFrame

Usage in notebook:
    import json_helper_basic as json_helper
    df_daily_summaries = json_helper.load_json_files_to_dataframe()
"""

import json
from pathlib import Path

import pandas as pd


def load_json_files_to_dataframe(directory_path=None) -> pd.DataFrame:
    """Load all JSON pages in data/daily_summaries into a single DataFrame."""
    if directory_path is None:
        directory_path = Path(__file__).parent / "data" / "daily_summaries"

    directory_path = Path(directory_path)
    if not directory_path.exists():
        raise FileNotFoundError(f"Directory not found: {directory_path}")

    # Read every JSON file and append records from payload['results'].
    rows = []
    for json_file in sorted(directory_path.glob("*.json")):
        if json_file.name == ".gitkeep":
            continue
        with open(json_file, "r") as file_handle:
            payload = json.load(file_handle)

        if isinstance(payload, dict) and isinstance(payload.get("results"), list):
            rows.extend(payload["results"])

    if not rows:
        raise ValueError("No JSON results found. Run tokengrabber_basic.py first.")

    return pd.DataFrame(rows)
