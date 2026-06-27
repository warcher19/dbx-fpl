"""
FPL API ingestion script — adapted from the existing api_calls.py.

Fetches bootstrap-static, element-summary (all players), and fixtures
from the FPL API and uploads JSON snapshots to the ADLS landing zone.

Runs as Task 1 in the fpl_api_ingestion Workflow job.
Requires env vars:
  - LANDING_ZONE_PATH : ADLS base path (e.g. abfss://fpl-landing@...)
  - FPL_API_BASE_URL  : https://fantasy.premierleague.com/api/
  - AZURE_STORAGE_CONNECTION_STRING or AZURE_CLIENT_ID/SECRET/TENANT for auth
"""
import asyncio
import json
import os
from datetime import datetime, timezone

import aiohttp
import requests
from azure.storage.blob import BlobServiceClient

BASE_URL = os.environ["FPL_API_BASE_URL"]
LANDING = os.environ["LANDING_ZONE_PATH"]
TIMESTAMP = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")

blob_client = BlobServiceClient.from_connection_string(
    os.environ["AZURE_STORAGE_CONNECTION_STRING"]
)


def _upload(container: str, blob_path: str, data: bytes) -> None:
    """Upload bytes to ADLS Gen2 via the Blob SDK."""
    client = blob_client.get_blob_client(container=container, blob=blob_path)
    client.upload_blob(data, overwrite=True)
    print(f"Uploaded: {blob_path}")


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
    container = "fpl-landing"

    # 1. Bootstrap-static
    print("Fetching bootstrap-static...")
    bootstrap = fetch_bootstrap()
    _upload(
        container,
        f"bootstrap-static/bootstrap-static_{TIMESTAMP}.json",
        json.dumps(bootstrap).encode(),
    )

    # 2. Fixtures
    print("Fetching fixtures...")
    fixtures = fetch_fixtures()
    _upload(
        container,
        f"fixtures/fixtures_{TIMESTAMP}.json",
        json.dumps(fixtures).encode(),
    )

    # 3. Element summaries — all active player IDs from bootstrap
    element_ids = [e["id"] for e in bootstrap["elements"]]
    print(f"Fetching {len(element_ids)} element summaries...")
    results = asyncio.run(fetch_all_elements(element_ids))

    # Bundle all element results into a single JSON array per snapshot
    payload = [{"element_id": eid, **data} for eid, data in results if data is not None]
    _upload(
        container,
        f"element-data/element_data_{TIMESTAMP}.json",
        json.dumps(payload).encode(),
    )
    print(f"Done — uploaded {len(payload)} element summaries.")


if __name__ == "__main__":
    main()
