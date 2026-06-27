"""
One-time script: download vaastav/Fantasy-Premier-League historical gameweek CSVs
and write them directly to the UC Volume landing zone.

Run this ONCE on a Databricks cluster (or via a one-off job) before the first
pipeline deployment. The Volume path /Volumes/fpl/bronze/landing/ is accessible
as a standard filesystem path — no storage SDK required.

The bronze Backfill Flow (once=True) in historical_gw_raw.py then processes the
files automatically on first pipeline run.

Seasons covered: 2016-17 through 2024-25 (9 complete seasons).
Each season's merged_gws.csv is written to:
  /Volumes/fpl/bronze/landing/historical/{season}/merged_gws.csv

Usage (on Databricks):
  %run scripts/download_vaastav_history
  # or: python scripts/download_vaastav_history.py
"""
import requests
from pathlib import Path

SEASONS = [
    "2016-17", "2017-18", "2018-19", "2019-20", "2020-21",
    "2021-22", "2022-23", "2023-24", "2024-25",
]
BASE_RAW_URL = (
    "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data"
)
LANDING = Path("/Volumes/fpl/bronze/landing")


def download_season(season: str) -> bytes | None:
    # Some seasons use merged_gws.csv, others (e.g. 2024-25) use merged_gw.csv
    for filename in ("merged_gws.csv", "merged_gw.csv"):
        url = f"{BASE_RAW_URL}/{season}/gws/{filename}"
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            print(f"  Downloaded {season} ({filename}): {len(resp.content):,} bytes")
            return resp.content
        except requests.HTTPError as exc:
            if exc.response.status_code == 404:
                continue  # try the alternate filename
            print(f"  WARNING: HTTP error for {season}/{filename}: {exc}")
            return None
        except requests.RequestException as exc:
            print(f"  WARNING: Failed to download {season}/{filename}: {exc}")
            return None
    print(f"  WARNING: No merged gameweek file found for {season}")
    return None


def main():
    print(f"Downloading {len(SEASONS)} seasons from vaastav/Fantasy-Premier-League...")
    success, skipped = 0, 0

    for season in SEASONS:
        print(f"\n[{season}]")
        data = download_season(season)
        if data:
            dest = LANDING / "historical" / season / "merged_gws.csv"
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(data)
            print(f"  Written → {dest}")
            success += 1
        else:
            skipped += 1

    print(f"\nComplete — {success} seasons written, {skipped} skipped.")
    print("You can now deploy and run the fpl_medallion pipeline.")
    print("The vaastav_backfill Append Flow will process the files on first run.")


if __name__ == "__main__":
    main()
