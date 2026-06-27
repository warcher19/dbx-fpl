"""
One-time script: download vaastav/Fantasy-Premier-League historical gameweek CSVs
and upload them to the ADLS landing zone for the bronze backfill flow.

Run this ONCE before the first pipeline deployment. The bronze Backfill Flow
(once=True) in historical_gw_raw.py then processes them automatically.

Seasons covered: 2016-17 through 2024-25 (9 complete seasons).
Each season's merged_gws.csv (~38 gameweeks × ~560 players) is uploaded as:
  historical/{season}/merged_gws.csv

Usage:
  python scripts/download_vaastav_history.py

Requires:
  - AZURE_STORAGE_CONNECTION_STRING env var
  - internet access to raw.githubusercontent.com
"""
import os
import requests
from azure.storage.blob import BlobServiceClient

SEASONS = [
    "2016-17", "2017-18", "2018-19", "2019-20", "2020-21",
    "2021-22", "2022-23", "2023-24", "2024-25",
]
BASE_RAW_URL = (
    "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data"
)
CONTAINER = "fpl-landing"

blob_client = BlobServiceClient.from_connection_string(
    os.environ["AZURE_STORAGE_CONNECTION_STRING"]
)


def download_season(season: str) -> bytes | None:
    """Download merged_gws.csv for a single season. Returns None on failure."""
    url = f"{BASE_RAW_URL}/{season}/gws/merged_gws.csv"
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        print(f"  Downloaded {season}: {len(resp.content):,} bytes")
        return resp.content
    except requests.RequestException as exc:
        print(f"  WARNING: Failed to download {season}: {exc}")
        return None


def upload(season: str, data: bytes) -> None:
    blob_path = f"historical/{season}/merged_gws.csv"
    client = blob_client.get_blob_client(container=CONTAINER, blob=blob_path)
    client.upload_blob(data, overwrite=True)
    print(f"  Uploaded  → {blob_path}")


def main():
    print(f"Downloading {len(SEASONS)} seasons from vaastav/Fantasy-Premier-League...")
    success, skipped = 0, 0
    for season in SEASONS:
        print(f"\n[{season}]")
        data = download_season(season)
        if data:
            upload(season, data)
            success += 1
        else:
            skipped += 1

    print(f"\nComplete — {success} seasons uploaded, {skipped} skipped.")
    print("You can now deploy and run the fpl_medallion pipeline.")
    print("The vaastav_backfill Append Flow will process the files on first run.")


if __name__ == "__main__":
    main()
