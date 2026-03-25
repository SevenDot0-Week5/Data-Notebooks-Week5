"""NOAA data fetch script.

This module downloads Daily Summaries data from NOAA and stores each page
response as a JSON file under data/daily_summaries.

Design notes:
- Uses urllib from Python standard library (no external HTTP package required).
- Uses pagination (offset/limit) until the final page is reached.
- Returns saved file paths so other modules can chain this step.
"""

import json
import os
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


BASE_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"


def get_token_or_help() -> str:
    """Return NOAA token or print clear setup instructions."""
    token = os.getenv("NOAA_TOKEN")
    if token:
        return token

    print("\nNOAA_TOKEN is not set.\n")

    if sys.stdin.isatty():
        typed = input("Paste your NOAA token (or press Enter to see setup steps): ").strip()
        if typed:
            return typed

    print("How to fix:")
    print("1) One-time run:")
    print('   NOAA_TOKEN="your_token_here" python tokengrabber.py')
    print("2) Or set for this terminal session:")
    print('   export NOAA_TOKEN="your_token_here"')
    print("   python tokengrabber.py")
    print("3) Optional: persist in zsh:")
    print('   echo \'export NOAA_TOKEN="your_token_here"\' >> ~/.zshrc')
    print("   source ~/.zshrc\n")

    raise ValueError("Missing NOAA_TOKEN")


def fetch_daily_summaries(
    token,
    output_dir=None,
    datasetid="GHCND",
    locationid="FIPS:10003",
    startdate="2018-01-01",
    enddate="2018-01-31",
    limit=1000,
):
    """Fetch NOAA daily summaries and persist paginated responses to JSON files.

    Args:
        token: NOAA API token.
        output_dir: Target directory for saved JSON pages.
        datasetid/locationid/startdate/enddate/limit: NOAA query parameters.

    Returns:
        list[Path]: Paths of JSON files written for each fetched page.
    """
    if not token:
        raise ValueError("Missing NOAA token. Pass token=... or set NOAA_TOKEN.")

    if output_dir is None:
        output_dir = Path(__file__).parent / "data" / "daily_summaries"
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    headers = {"token": token}
    offset = 1
    page = 0
    saved_files = []

    while True:
        # Build one paginated request.
        params = {
            "datasetid": datasetid,
            "locationid": locationid,
            "startdate": startdate,
            "enddate": enddate,
            "limit": limit,
            "offset": offset,
        }
        query_url = f"{BASE_URL}?{urlencode(params)}"
        request = Request(query_url, headers=headers, method="GET")
        try:
            with urlopen(request, timeout=30) as response:
                status_code = getattr(response, "status", None)
                if status_code is not None and status_code >= 400:
                    raise RuntimeError(f"NOAA request failed with status {status_code}")
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            print(f"NOAA API request failed (HTTP {exc.code}).")
            if exc.code in (401, 403):
                print("Your token may be missing, invalid, or not authorized.")
            raise
        except URLError:
            print("Could not reach NOAA API. Check internet connection and try again.")
            raise
        results = payload.get("results", []) if isinstance(payload, dict) else []

        # Save each page so source data remains inspectable and reproducible.
        filename = f"daily_summaries_{locationid.replace(':', '')}_{startdate}_{enddate}_{page}.json"
        out_file = output_dir / filename
        with open(out_file, "w") as file_handle:
            json.dump(payload, file_handle, indent=2)
        saved_files.append(out_file)

        # Stop when NOAA returned fewer rows than limit (last page).
        if not results or len(results) < limit:
            break

        # Continue pagination.
        offset += limit
        page += 1

    return saved_files


if __name__ == "__main__":
    # CLI mode: read token from environment and fetch into default folder.
    token = get_token_or_help()
    files = fetch_daily_summaries(token=token)
    print(f"✅ Data saved: {len(files)} file(s)")
    for file_path in files:
        print(f" - {file_path}")