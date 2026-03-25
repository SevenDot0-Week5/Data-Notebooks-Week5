"""Command-line runner for creating NOAA Daily Summaries DataFrame.

Run this file when you want a quick sanity check of the end-to-end data flow
without opening the notebook or API UI.
"""

import os

import json_helper


def main():
    # If NOAA_TOKEN is set, fetch latest data then load DataFrame.
    # If NOAA_TOKEN is not set, load from existing local JSON files.
    token = os.getenv("NOAA_TOKEN")
    if token:
        df_daily_summaries = json_helper.fetch_and_load_daily_summaries_dataframe(token=token)
    else:
        df_daily_summaries = json_helper.load_json_files_to_dataframe()

    # Print a compact summary so you can verify shape/schema quickly.
    print("DataFrame created successfully")
    print(f"Rows: {len(df_daily_summaries)}")
    print(f"Columns: {list(df_daily_summaries.columns)}")
    print(df_daily_summaries.head())


if __name__ == "__main__":
    # Entry point for direct execution: python build_daily_summaries_df.py
    main()
