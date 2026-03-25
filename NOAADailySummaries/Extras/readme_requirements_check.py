"""Quick checker for NOAA Daily Summaries README requirements.

Run:
  python readme_requirements_check.py

It validates:
1) Required JSON files exist
2) json_helper_basic can build a DataFrame
3) Required columns for notebook exercises are present
"""

from pathlib import Path

import json_helper_basic


REQUIRED_FILES = [
    "daily_summaries_FIPS10003_jan_2018_0.json",
    "daily_summaries_FIPS10003_jan_2018_1.json",
]
REQUIRED_COLUMNS = {"date", "datatype", "value"}


def main() -> None:
    base_dir = Path(__file__).parent / "data" / "daily_summaries"

    print("[1/3] Checking required files...")
    missing = [name for name in REQUIRED_FILES if not (base_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"Missing files: {missing}")
    print("PASS: required JSON files exist")

    print("[2/3] Building DataFrame with json_helper_basic...")
    df = json_helper_basic.load_json_files_to_dataframe(base_dir)
    print(f"PASS: DataFrame built with {len(df)} rows")

    print("[3/3] Checking required columns for notebook steps...")
    missing_cols = REQUIRED_COLUMNS - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing DataFrame columns: {sorted(missing_cols)}")
    print(f"PASS: Required columns present: {sorted(REQUIRED_COLUMNS)}")

    print("\nREADME_REQUIREMENTS_CHECK_PASS")


if __name__ == "__main__":
    main()
