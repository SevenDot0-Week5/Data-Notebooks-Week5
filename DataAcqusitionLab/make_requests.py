# make_requests.py

import json
import time
import urllib.parse
import urllib.request
from urllib.error import HTTPError, URLError

BASE_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2/locations"
TOKEN = "zNUiHJIxXPuvUrhoxkpVVqDSQAghWMpV"

LIMIT = 1000
TOTAL_FILES = 39
DELAY_SECONDS = 1
MAX_RETRIES = 5
RETRY_WAIT_SECONDS = 3


def fetch_locations_page(offset: int, limit: int, token: str) -> dict:
    params = urllib.parse.urlencode({
        "limit": limit,
        "offset": offset,
    })
    url = f"{BASE_URL}?{params}"

    request = urllib.request.Request(url)
    request.add_header("token", token)

    with urllib.request.urlopen(request) as response:
        data = response.read().decode("utf-8")
        return json.loads(data)


def fetch_with_retry(offset: int, limit: int, token: str) -> dict:
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"Requesting offset={offset} (attempt {attempt}/{MAX_RETRIES})")
            return fetch_locations_page(offset, limit, token)

        except HTTPError as e:
            last_error = e

            if e.code == 503:
                print(f"HTTP 503: Service Unavailable. Waiting {RETRY_WAIT_SECONDS} seconds, then retrying...")
                time.sleep(RETRY_WAIT_SECONDS)
            elif e.code == 429:
                print(f"HTTP 429: Too Many Requests. Waiting {RETRY_WAIT_SECONDS} seconds, then retrying...")
                time.sleep(RETRY_WAIT_SECONDS)
            elif e.code == 401:
                raise Exception(
                    "HTTP 401: Unauthorized. Your NOAA token is missing or invalid."
                ) from e
            else:
                raise Exception(f"HTTP Error {e.code}: {e.reason}") from e

        except URLError as e:
            last_error = e
            print(f"Network error: {e.reason}. Waiting {RETRY_WAIT_SECONDS} seconds, then retrying...")
            time.sleep(RETRY_WAIT_SECONDS)

    raise Exception(f"Request failed after {MAX_RETRIES} attempts.") from last_error


def save_json_file(data: dict, file_index: int) -> None:
    filename = f"locations_{file_index}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

    print(f"Saved {filename}")


def main() -> None:
    for file_index in range(TOTAL_FILES):
        offset = file_index * LIMIT + 1

        try:
            data = fetch_with_retry(offset=offset, limit=LIMIT, token=TOKEN)
            save_json_file(data, file_index)

            if file_index < TOTAL_FILES - 1:
                print(f"Waiting {DELAY_SECONDS} second(s) before next request...\n")
                time.sleep(DELAY_SECONDS)

        except Exception as e:
            print(f"Stopped on file_index={file_index}, offset={offset}")
            print(f"Reason: {e}")
            break

    print("Script finished.")


if __name__ == "__main__":
    main()