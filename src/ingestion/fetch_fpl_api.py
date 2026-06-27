"""
FPL API ingestion script — adapted from the existing api_calls.py.

Fetches bootstrap-static, element-summary (all players), and fixtures
from the FPL API and writes JSON snapshots directly to the UC Volume
landing zone at /Volumes/fpl/bronze/landing/.

Runs as Task 1 in the fpl_api_ingestion Workflow job on Databricks.
The Volume path is accessible as a standard filesystem path on any
Databricks cluster or serverless context — no storage SDK required.

Requires env var:
  - FPL_API_BASE_URL : https://fantasy.premierleague.com/api/
"""
import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import requests

BASE_URL  = os.environ.get("FPL_API_BASE_URL", "https://fantasy.premierleague.com/api/")
LANDING   = Path("/Volumes/fpl/bronze/landing")
TIMESTAMP = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")


def _write(sub_path: str, data: bytes) -> None:
    """Write bytes to a path under the UC Volume landing zone."""
    dest = LANDING / sub_path
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(data)
    print(f"Written: {dest}")


def fetch_bootstrap() -> dict:
    resp = requests.get(f"{BASE_URL}bootstrap-static/", timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_fixtures() -> list:
    resp = requests.get(f"{BASE_URL}fixtures/", timeout=30)
    resp.raise_for_status()
    return resp.json()


async def _fetch_element(session: aiohttp.ClientSession, element_id: int) -> tuple:
    url = f"{BASE_URL}element-summary/{element_id}/"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return element_id, data
    except Exception as exc:
        print(f"Warning: failed to fetch element {element_id}: {exc}")
        return element_id, None


async def fetch_all_elements(element_ids: list) -> list:
    """Async fan-out — reuses existing api_calls.py pattern."""
    async with aiohttp.ClientSession() as session:
        tasks = [_fetch_element(session, eid) for eid in element_ids]
        return await asyncio.gather(*tasks)


def main():
    # 1. Bootstrap-static
    print("Fetching bootstrap-static...")
    bootstrap = fetch_bootstrap()
    _write(
        f"bootstrap-static/bootstrap-static_{TIMESTAMP}.json",
        json.dumps(bootstrap).encode(),
    )

    # 2. Fixtures
    print("Fetching fixtures...")
    fixtures = fetch_fixtures()
    _write(
        f"fixtures/fixtures_{TIMESTAMP}.json",
        json.dumps(fixtures).encode(),
    )

    # 3. Element summaries — all active player IDs from bootstrap
    element_ids = [e["id"] for e in bootstrap["elements"]]
    print(f"Fetching {len(element_ids)} element summaries...")
    results = asyncio.run(fetch_all_elements(element_ids))

    payload = [{"element_id": eid, **data} for eid, data in results if data is not None]
    _write(
        f"element-data/element_data_{TIMESTAMP}.json",
        json.dumps(payload).encode(),
    )
    print(f"Done — written {len(payload)} element summaries.")


if __name__ == "__main__":
    main()
