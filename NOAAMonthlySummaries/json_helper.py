"""Data loading helpers for NOAA Daily Summaries.

This module is the shared bridge between raw JSON pages and tabular analysis.
Both scripts, notebooks, and API code call these functions to avoid duplicate
parsing logic.
"""

import json
import pandas as pd
import os
from pathlib import Path

from tokengrabber import fetch_daily_summaries


def load_json_files_to_dataframe(directory_path=None):
    """
    Load all JSON files from a directory into a pandas DataFrame.
    
    Args:
        directory_path: Path to directory containing JSON files.
                       If None, uses ./data/monthly_summaries/
    
    Returns:
        pandas.DataFrame: DataFrame containing all JSON data flattened
    """
    # Default to this project data folder so callers can omit a path.
    if directory_path is None:
        directory_path = Path(__file__).parent / 'data' / 'monthly_summaries'
    
    directory_path = Path(directory_path)
    
    if not directory_path.exists():
        raise FileNotFoundError(f"Directory not found: {directory_path}")
    
    all_data = []
    
    # Find all JSON files
    json_files = list(directory_path.glob('*.json'))
    
    if not json_files:
        raise FileNotFoundError(f"No JSON files found in {directory_path}")
    
    print(f"Found {len(json_files)} JSON files")
    
    for json_file in json_files:
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)
                
            # Handle both single object and array of objects
            if isinstance(data, list):
                all_data.extend(data)
            elif isinstance(data, dict):
                # If dict has a 'results' key with list data, use that
                if 'results' in data and isinstance(data['results'], list):
                    all_data.extend(data['results'])
                # Otherwise only append if dict already looks like a single record.
                elif {'date', 'datatype', 'value'}.issubset(data.keys()):
                    all_data.append(data)
        except json.JSONDecodeError as e:
            # Keep going if one page is malformed; report and continue.
            print(f"Warning: Could not parse {json_file}: {e}")
            continue
    
    if not all_data:
        raise ValueError("No data found in JSON files")
    
    # Convert to DataFrame
    df = pd.DataFrame(all_data)
    
    print(f"Loaded {len(df)} records into DataFrame")
    return df


def fetch_and_load_daily_summaries_dataframe(
    token='GQSNnsbUslNHBAPVDfWdwswxGsQNiutO',
    directory_path=None,
    datasetid="GSOM",
    locationid="FIPS:10003",
    startdate="1938-01-01",
    enddate="2018-01-31",
    limit=1000,
    OFFSET=1,
    DATATYPEID="TAVG",
):
    """Fetch latest NOAA pages and return a freshly loaded DataFrame.

    This is the convenience function for one-call refresh + load behavior.
    """
    if token is None:
        token = os.getenv("NOAA_TOKEN")

    if directory_path is None:
        directory_path = Path(__file__).parent / 'data' / 'monthly_summaries'

    fetch_daily_summaries(
        token=token,
        output_dir=directory_path,
        datasetid=datasetid,
        locationid=locationid,
        startdate=startdate,
        enddate=enddate,
        limit=limit,
        OFFSET=OFFSET,
        DATATYPEID=DATATYPEID,
    )
    # Always return DataFrame from disk so behavior matches notebook/API loaders.
    return load_json_files_to_dataframe(directory_path)


def fetch_and_load_chunked_dataframe(
    start_year=1938,
    end_year=2026,
    chunk_size=5,
    token='GQSNnsbUslNHBAPVDfWdwswxGsQNiutO',
    directory_path=None,
    datasetid="GSOM",
    locationid="FIPS:10003",
    limit=1000,
    OFFSET=1,
    DATATYPEID="TAVG",
):
    """Fetch data in year chunks to handle large date ranges without errors.
    
    Args:
        start_year: Starting year
        end_year: Ending year
        chunk_size: Number of years per chunk
        token: NOAA API token
        directory_path: Output directory for JSON files
        datasetid: NOAA dataset ID (e.g., "GSOM", "GHCND")
        locationid: Location ID (e.g., "FIPS:10003")
        limit: Records per API page
        OFFSET: API offset parameter
        DATATYPEID: Data type ID (e.g., "TAVG")
    
    Returns:
        pandas.DataFrame: Combined data from all chunks
    """
    if token is None:
        token = os.getenv("NOAA_TOKEN")

    if directory_path is None:
        directory_path = Path(__file__).parent / 'data' / 'monthly_summaries'

    all_dfs = []
    year = start_year

    while year <= end_year:
        chunk_end = min(year + chunk_size - 1, end_year)
        year_start = f"{year}-01-01"
        year_end = f"{chunk_end}-12-31"

        try:
            chunk_df = fetch_and_load_daily_summaries_dataframe(
                token=token,
                directory_path=directory_path,
                datasetid=datasetid,
                locationid=locationid,
                startdate=year_start,
                enddate=year_end,
                limit=limit,
                OFFSET=OFFSET,
                DATATYPEID=DATATYPEID,
            )
            all_dfs.append(chunk_df)
        except Exception:
            continue

        year = chunk_end + 1

    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    else:
        return pd.DataFrame()
